package ink.haifeng.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/5 16:49:24
 */
public class FlinkTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.useBlinkPlanner()
                //.inBatchMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("CREATE TABLE user_visit (" +
                " user STRING, ts BIGINT," +
                "et as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "WATERMARK FOR et as et-INTERVAL 2 SECOND " +
                ") WITH (" +
                "'connector'='filesystem'," +
                "'path'='data/user-visit.log'," +
                "'format'='csv'" +
                ")");
        tableEnv.executeSql("select * from user_visit").print();

        env.execute("table test");
    }
}
