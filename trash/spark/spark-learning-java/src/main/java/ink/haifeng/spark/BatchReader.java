package ink.haifeng.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/8/2 10:19:06
 */
public class BatchReader {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("batch_reader").getOrCreate();

        for (int i = 1; i < 3; i++) {
            Dataset<Row> dataset = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://172.16.2.8:3306/db_test?useUnicode=true&characterEncoding=utf-8")
                    //.option("dbtable", "db_test.tb_user" + i)
                    .option("query", "select  * from db_test.tb_user" + i)
                    .option("user", "root")
                    .option("password", "123456")
                    .load();
            dataset.show();
            dataset.show();
//            String savePath = "/Users/haifeng/workspace/Projects/amber/spark/spark-learning-java/spark-learning
//            -java" +
//                    "/data/tb_user" + i;

            //dataset.write().csv(savePath);
            // dataset.write().text(savePath);
            // dataset.toDF().write().text(savePath);

            dataset.toDF().write().saveAsTable("");

        }
        spark.stop();
    }
}
