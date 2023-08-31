package org.example;


import org.apache.spark.sql.SparkSession;


public class SparkPaimon {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse", "file:/Users/haifeng/workspace/amber/paimon/paimon_warehouse")
                .master("local")
                .getOrCreate();
        spark.sql("use paimon");
        String sql = "create table my_table (\n" +
                "    k int,\n" +
                "    v string\n" +
                ") tblproperties (\n" +
                "    'primary-key' = 'k'\n" +
                ");";
        spark.sql(sql);
        spark.sql("INSERT INTO my_table VALUES (1, 'Hi'), (2, 'Hello');");
        spark.sql("SELECT * FROM my_table").show();
    }
}
