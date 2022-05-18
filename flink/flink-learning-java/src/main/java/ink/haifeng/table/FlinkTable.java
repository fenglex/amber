package ink.haifeng.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/5 16:49:24
 */
public class FlinkTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.useBlinkPlanner()
                //.inBatchMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE user_visit (" +
                " `user` STRING, ts BIGINT," +
                " et as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                " window_start, " +
                " WATERMARK FOR et as et-INTERVAL '2' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = '/Users/haifeng/workspace/Projects/amber/flink/flink-learning-java/data/user-visit.csv', " +
                " 'format' = 'csv'," +
                "'csv.field-delimiter'=','" +
                ")");

        Table table = tableEnv.sqlQuery("select user ,count(1) from user_visit group by user");
        tableEnv.toChangelogStream(table).print();

        env.execute("table test");
    }
}
