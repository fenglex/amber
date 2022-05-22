package ink.haifeng;

import cn.hutool.core.thread.ThreadUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Int;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<UserName> stream1 = env.addSource(new NameSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserName>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<UserName>() {
                            @Override
                            public long extractTimestamp(UserName userName, long l) {
                                return userName.getTm() * 1000L;
                            }
                        }));

        SingleOutputStreamOperator<UserAge> socketStream = env.socketTextStream("localhost", 7777).map(e -> {
            String[] split = e.split(",");
            return new UserAge(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserAge>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<UserAge>() {
            @Override
            public long extractTimestamp(UserAge userAge, long l) {
                return userAge.tm * 1000L;
            }
        }));
        stream1.keyBy(UserName::getName).window(TumblingEventTimeWindows.of(Time.seconds(5))).reduce(new ReduceFunction<UserName>() {
                    @Override
                    public UserName reduce(UserName v1, UserName v2) throws Exception {
                        return v1.tm > v2.tm ? v1 : v2;
                    }
                }).keyBy(UserName::getUser)
                .intervalJoin(socketStream.keyBy(UserAge::getUser))
                .between(Time.seconds(-5), Time.seconds(0))
                .process(new ProcessJoinFunction<UserName, UserAge, String>() {
                    @Override
                    public void processElement(UserName left, UserAge right, ProcessJoinFunction<UserName, UserAge,
                            String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "____" + right);
                    }
                }).print("out");

        env.execute("interval join job");
    }


    private static class NameSource implements SourceFunction<UserName> {
        private boolean skip = false;

        @Override
        public void run(SourceContext<UserName> ctx) throws Exception {
            for (int i = 0; i < 1000; i++) {
                UserName user = new UserName();
                user.setUser("u" + i);
                user.setName("user" + i);
                user.setTm(i);
                ctx.collect(user);
                System.out.println("output:" + user);
                ThreadUtil.sleep(5, TimeUnit.SECONDS);
                if (skip) {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            skip = true;
        }


    }


    private static class UserName {
        public UserName(String user, String name, int tm) {
            this.user = user;
            this.name = name;
            this.tm = tm;
        }

        @Override
        public String toString() {
            return "UserName{" +
                    "user='" + user + '\'' +
                    ", name='" + name + '\'' +
                    ", tm=" + tm +
                    '}';
        }

        String user;
        String name;
        int tm;

        public int getTm() {
            return tm;
        }

        public void setTm(int tm) {
            this.tm = tm;
        }

        public UserName() {
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static class UserAge {

        @Override
        public String toString() {
            return "UserAge{" +
                    "user='" + user + '\'' +
                    ", age=" + age +
                    ", tm=" + tm +
                    '}';
        }

        public UserAge(String user, int age, int tm) {
            this.user = user;
            this.age = age;
            this.tm = tm;
        }

        String user;
        int age;

        int tm;

        public int getTm() {
            return tm;
        }

        public void setTm(int tm) {
            this.tm = tm;
        }

        public UserAge() {
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
