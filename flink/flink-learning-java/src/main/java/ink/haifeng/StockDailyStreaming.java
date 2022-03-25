package ink.haifeng;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class StockDailyStreaming {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> data = env.readTextFile("/Users/haifeng/Documents/stock-20220322.csv");
        SingleOutputStreamOperator<StockData> reduce = data.map(StockData::new)
                .filter(e -> e.getStockCode().startsWith("00") || e.getStockCode().startsWith("60") || e.getStockCode().startsWith("68") || e.getStockCode().startsWith("30"))
                .filter(e -> e.getTradeDay().equals("20220322"))
                .filter(e -> e.getRealtime() / 100000 <= 1500)
                .keyBy(StockData::getStockCode)
                .reduce((d1, d2) -> d1.getRealtime() >= d2.getRealtime() ? d1 : d2);

        //reduce.print("Test");
        String insertSql = "INSERT INTO tb_stock_eod(stock_code,trade_day,close_price,high_price,low_price," +
                "open_price,pre_close_price,volume,create_time,update_time) VALUES(?,?,?,?,?,?,?,?,?,?)";
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/db_quotation")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

        SinkFunction<StockData> sink = JdbcSink.sink(insertSql, (preparedStatement, e) -> {
            preparedStatement.setString(1, e.getStockCode());
            preparedStatement.setString(2, e.getTradeDay());
            preparedStatement.setString(3, e.getPrice() + "");
            preparedStatement.setString(4, e.getHigh() + "");
            preparedStatement.setString(5, e.getLow() + "");
            preparedStatement.setString(6, e.getOpen() + "");
            preparedStatement.setString(7, e.getPrice() + "");
            preparedStatement.setString(8, e.getVolume() + "");
            preparedStatement.setString(9, System.currentTimeMillis() / 1000 + "");
            preparedStatement.setString(10, System.currentTimeMillis() / 1000 + "");
        }, executionOptions, connectionOptions);

        //reduce.addSink(sink);
       //exactlyOnceSink(reduce, insertSql);
        final MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
        xaDataSource.setUrl("jdbc:mysql://localhost:3306");
        xaDataSource.setUser("root");
        xaDataSource.setPassword("123456");


        reduce.addSink(JdbcSink.exactlyOnceSink(insertSql, (preparedStatement, e) -> {
                    preparedStatement.setString(1, e.getStockCode());
                    preparedStatement.setString(2, e.getTradeDay());
                    preparedStatement.setString(3, e.getPrice() + "");
                    preparedStatement.setString(4, e.getHigh() + "");
                    preparedStatement.setString(5, e.getLow() + "");
                    preparedStatement.setString(6, e.getOpen() + "");
                    preparedStatement.setString(7, e.getPrice() + "");
                    preparedStatement.setString(8, e.getVolume() + "");
                    preparedStatement.setString(9, System.currentTimeMillis() / 1000 + "");
                    preparedStatement.setString(10, System.currentTimeMillis() / 1000 + "");
                }, JdbcExecutionOptions.builder()
                        .withMaxRetries(0)
                        .build(),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    // create a driver-specific XA DataSource
                    // The following example is for derby
                    xaDataSource.setDatabaseName("db_quotation");
                    return xaDataSource;
                }));
        env.execute("StockEod");
    }

    public static void exactlyOnceSink(SingleOutputStreamOperator<StockData> reduce, String insertSql) {

        final MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
        xaDataSource.setUrl("jdbc:mysql://localhost:3306");
        xaDataSource.setUser("root");
        xaDataSource.setPassword("123456");


        reduce.addSink(JdbcSink.exactlyOnceSink(insertSql, (preparedStatement, e) -> {
                    preparedStatement.setString(1, e.getStockCode());
                    preparedStatement.setString(2, e.getTradeDay());
                    preparedStatement.setString(3, e.getPrice() + "");
                    preparedStatement.setString(4, e.getHigh() + "");
                    preparedStatement.setString(5, e.getLow() + "");
                    preparedStatement.setString(6, e.getOpen() + "");
                    preparedStatement.setString(7, e.getPrice() + "");
                    preparedStatement.setString(8, e.getVolume() + "");
                    preparedStatement.setString(9, System.currentTimeMillis() / 1000 + "");
                    preparedStatement.setString(10, System.currentTimeMillis() / 1000 + "");
                }, JdbcExecutionOptions.builder()
                        .withMaxRetries(0)
                        .build(),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    // create a driver-specific XA DataSource
                    // The following example is for derby
                    xaDataSource.setDatabaseName("db_quotation");
                    return xaDataSource;
                }));

    }
}
