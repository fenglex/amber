package ink.haifeng.quotation.sink;

import ink.haifeng.quotation.model.dto.ProductQuotation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;

/**
 * 保存产品分钟数据
 */
public class ProductQuotationSink implements Sink<ProductQuotation> {

    private final Properties properties;

    public ProductQuotationSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public SinkFunction<ProductQuotation> sink() {
        String sql = "INSERT INTO tb_product_index_quotation" + "(product_code,product_name,trade_day,trade_time," +
                "price,pre_close,gain,`change`,amount,total_amount,create_time,update_time) " + "VALUES(?,?,?,?,?," +
                "?,?,?,?,?,?,?) on duplicate key update " + "product_name=?,price=?,pre_close=?,gain=?,`change`=?," +
                "amount=?,total_amount=?,update_time=?";
        return JdbcSink.sink(sql, (JdbcStatementBuilder<ProductQuotation>) (ps, e) -> {
            ps.setString(1, e.getProductCode());
            ps.setString(2, e.getProductName());
            ps.setInt(3, e.getTradeDay());
            ps.setInt(4, e.getTradeTime());
            ps.setBigDecimal(5, e.getPrice());
            ps.setBigDecimal(6, e.getPreClose());
            BigDecimal price = e.getPrice();
            BigDecimal preClosePrice = e.getPreClose();
            BigDecimal gain = null, change = null;
            if (preClosePrice != null) {
                gain = price.divide(preClosePrice, 4, RoundingMode.HALF_UP).subtract(new BigDecimal("1"));
                change = price.subtract(preClosePrice);
            }
            ps.setBigDecimal(7, gain);
            ps.setBigDecimal(8, change);
            ps.setLong(9, e.getAmount());
            ps.setLong(10, e.getTotalAmount());
            int time = (int) (System.currentTimeMillis() / 1000);
            ps.setInt(11, time);
            ps.setInt(12, time);
            ps.setString(13, e.getProductName());
            ps.setBigDecimal(14, e.getPrice());
            ps.setBigDecimal(15, e.getPreClose());
            ps.setBigDecimal(16, gain);
            ps.setBigDecimal(17, change);
            ps.setLong(18, e.getAmount());
            ps.setLong(19, e.getTotalAmount());
            ps.setInt(20, time);
        }, JdbcExecutionOptions.builder().withBatchSize(500).withBatchIntervalMs(200).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(properties.getProperty("jdbc.url")).withDriverName("com.mysql.cj.jdbc.Driver").withUsername(properties.getProperty("jdbc.user")).withPassword(properties.getProperty("jdbc.password")).build());
    }
}
