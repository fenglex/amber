package ink.haifeng.quotation.sink;

import ink.haifeng.quotation.model.entity.StockQuotation;
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
 * @date Created in 2022/4/29 18:15:02
 */
public class StockQuotationSink implements Sink<StockQuotation> {

    private final Properties properties;

    public StockQuotationSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public SinkFunction<StockQuotation> sink() {
        String sql = "INSERT INTO tb_stock_quotation(stock_code,trade_day,trade_time,price,pre_close,gain,`change`," +
                "volume,total_volume,amount,total_amount,real_time,create_time,update_time) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update " +
                "price=?,pre_close=?,gain=?,`change`=?,volume=?,total_volume=?,amount=?," +
                "total_amount=?,real_time=?,create_time=?,update_time=?";
        return JdbcSink.sink(sql, (JdbcStatementBuilder<StockQuotation>) (ps, quotation) -> {
            ps.setString(1, quotation.getStockCode());
            ps.setInt(2, quotation.getTradeDay());
            ps.setInt(3, quotation.getTradeTime());
            ps.setBigDecimal(4, quotation.getPrice());
            ps.setBigDecimal(5, quotation.getPreClose());
            ps.setBigDecimal(6, quotation.getGain());
            ps.setBigDecimal(7, quotation.getChange());
            ps.setLong(8, quotation.getVolume());
            ps.setLong(9, quotation.getTotalVolume());
            ps.setLong(10, quotation.getAmount());
            ps.setLong(11, quotation.getTotalVolume());
            ps.setInt(12, quotation.getRealTime());
            ps.setInt(13, quotation.getCreateTime());
            ps.setInt(14, quotation.getUpdateTime());
            ps.setBigDecimal(15, quotation.getPrice());
            ps.setBigDecimal(16, quotation.getPreClose());
            ps.setBigDecimal(17, quotation.getGain());
            ps.setBigDecimal(18, quotation.getChange());
            ps.setLong(19, quotation.getVolume());
            ps.setLong(20, quotation.getTotalVolume());
            ps.setLong(21, quotation.getAmount());
            ps.setLong(22, quotation.getTotalVolume());
            ps.setInt(23, quotation.getRealTime());
            ps.setInt(24, quotation.getCreateTime());
            ps.setInt(25, quotation.getUpdateTime());
        }, JdbcExecutionOptions.builder().withBatchSize(500)
                .withBatchIntervalMs(200).build(), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(properties.getProperty("jdbc.url"))
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(properties.getProperty("jdbc.user"))
                .withPassword(properties.getProperty("jdbc.password"))
                .build());
    }
}
