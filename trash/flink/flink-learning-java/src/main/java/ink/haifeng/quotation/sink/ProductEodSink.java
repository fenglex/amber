package ink.haifeng.quotation.sink;

import ink.haifeng.quotation.model.entity.ProductEod;
import ink.haifeng.quotation.model.entity.StockDaily;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

public class ProductEodSink implements Sink<ProductEod> {

    private final Properties properties;

    public ProductEodSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public SinkFunction<ProductEod> sink() {
        String sql =
                "INSERT INTO tb_product_index_eod(product_code,product_name,trade_day,close_price,high_price," +
                        "low_price,open_price,pre_close_price,adj_factor,amount,create_time,update_time) "
                        + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update " +
                        "product_name=?,close_price=?," +
                        "high_price=?,low_price=?,open_price=?,pre_close_price=?,adj_factor=?,amount=?," +
                        "update_time=?";
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl(properties.getProperty("jdbc.url"))
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(properties.getProperty("jdbc.user"))
                .withPassword(properties.getProperty("jdbc.password")).build();

        return JdbcSink.sink(sql, (JdbcStatementBuilder<ProductEod>) (ps, e) -> {
            ps.setString(1, e.getProductCode());
            ps.setString(2, e.getProductName());
            ps.setInt(3, e.getTradeDay());
            ps.setBigDecimal(4, e.getClosePrice());
            ps.setBigDecimal(5, e.getHighPrice());
            ps.setBigDecimal(6, e.getLowPrice());
            ps.setBigDecimal(7, e.getOpenPrice());
            ps.setBigDecimal(8, e.getPreClosePrice());
            ps.setBigDecimal(9, e.getAdjFactor());
            ps.setLong(10, e.getAmount());
            int time = (int) (System.currentTimeMillis() / 1000);
            ps.setLong(11, time);
            ps.setLong(12, time);
            ps.setString(13, e.getProductName());
            ps.setBigDecimal(14, e.getClosePrice());
            ps.setBigDecimal(15, e.getHighPrice());
            ps.setBigDecimal(16, e.getLowPrice());
            ps.setBigDecimal(17, e.getOpenPrice());
            ps.setBigDecimal(18, e.getPreClosePrice());
            ps.setBigDecimal(19, e.getAdjFactor());
            ps.setLong(20, e.getAmount());
            ps.setLong(21, time);
        }, JdbcExecutionOptions.builder().withBatchSize(500).withBatchIntervalMs(200).build(), connectionOptions);
    }
}
