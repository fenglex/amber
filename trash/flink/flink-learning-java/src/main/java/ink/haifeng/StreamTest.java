package ink.haifeng;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.jws.soap.SOAPBinding;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 10:34:11
 */
public class StreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 8900);


        textStream.map((MapFunction<String, User>) value -> {
            String[] split = value.split(",");
            return new User(split[0], Integer.parseInt(split[1]));
        }).keyBy(User::getName).reduce(new ReduceFunction<User>() {
            @Override
            public User reduce(User v1, User v2) throws Exception {
                System.out.println(v1 + "--->" + v2);
                return v2;
            }
        }).print();

        env.execute();
    }

    private static class User {
        String name;
        int age;

        @Override
        public String toString() {
            return "User{" + "name='" + name + '\'' + ", age=" + age + '}';
        }

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public User() {

        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
