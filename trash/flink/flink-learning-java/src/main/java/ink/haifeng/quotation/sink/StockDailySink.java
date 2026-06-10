package ink.haifeng.quotation.sink;


import ink.haifeng.quotation.model.entity.StockDaily;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/29 18:16:31
 */
public class StockDailySink implements Sink<StockDaily>{

    private final Properties properties;

    public StockDailySink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public SinkFunction<StockDaily> sink() {
        String sql = "INSERT INTO tb_stock_eod(stock_code,trade_day,close_price,high_price,low_price,open_price," +
                "pre_close_price,volume,create_time,update_time) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?) on duplicate key update " +
                "close_price=?,high_price=?,low_price=?,open_price=?,pre_close_price=?,volume=?,create_time=?," +
                "update_time=?";
        return JdbcSink.sink(sql, (JdbcStatementBuilder<StockDaily>) (ps, e) -> {
            ps.setString(1, e.getStockCode());
            ps.setInt(2, e.getTradeDay());
            ps.setBigDecimal(3, e.getClosePrice());
            ps.setBigDecimal(4, e.getHighPrice());
            ps.setBigDecimal(5, e.getLowPrice());
            ps.setBigDecimal(6, e.getOpenPrice());
            ps.setBigDecimal(7, e.getPreClosePrice());
            ps.setLong(8, e.getVolume());
            ps.setLong(9, e.getCreateTime());
            ps.setLong(10, e.getUpdateTime());
            ps.setBigDecimal(11, e.getClosePrice());
            ps.setBigDecimal(12, e.getHighPrice());
            ps.setBigDecimal(13, e.getLowPrice());
            ps.setBigDecimal(14, e.getOpenPrice());
            ps.setBigDecimal(15, e.getPreClosePrice());
            ps.setLong(16, e.getVolume());
            ps.setLong(17, e.getCreateTime());
            ps.setLong(18, e.getUpdateTime());
        }, JdbcExecutionOptions.builder().withBatchSize(500)
                .withBatchIntervalMs(200).build(), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(properties.getProperty("jdbc.url"))
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(properties.getProperty("jdbc.user"))
                .withPassword(properties.getProperty("jdbc.password"))
                .build());
    }
}
