package ink.haifeng.quotation.source;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import ink.haifeng.quotation.model.dto.ProductConstituentsInfo;
import ink.haifeng.quotation.model.entity.ProductIndexBasicInfo;
import ink.haifeng.quotation.model.entity.ProductIndexConstituents;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 17:58:14
 */
public class ProductConstituentsSource extends RichSourceFunction<List<ProductConstituentsInfo>> {
    @Override
    public void run(SourceContext<List<ProductConstituentsInfo>> ctx) throws Exception {
        ExecutionConfig.GlobalJobParameters parameters =
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParameterTool params = (ParameterTool) parameters;
        SimpleDataSource source = new SimpleDataSource(params.get("jdbc.url"), params.get("jdbc.user"),
                params.get("jdbc.password"));
        Db db = Db.use(source);
        List<ProductConstituentsInfo> constituents = new ArrayList<>(50000);
        String sql = "SELECT \n" +
                "    t1.trade_day,\n" +
                "    t1.last_trade_day,\n" +
                "    t1.product_code,\n" +
                "    t1.product_name,\n" +
                "    t1.adj_mkt_cap,\n" +
                "    t1.last_adj_mkt_cap,\n" +
                "    t1.divisor,\n" +
                "    t1.last_divisor,\n" +
                "    t1.close_price,\n" +
                "    t1.valid,\n" +
                "    t2.adj_share,\n" +
                "    t2.stock_abbr,\n" +
                "    t2.stock_code\n" +
                "FROM\n" +
                "    tb_product_index_basicinfo t1\n" +
                "        LEFT JOIN\n" +
                "    tb_product_index_constituents t2 ON t1.trade_day = t2.trade_day\n" +
                "        AND t1.product_code = t2.product_code \n" +
                "        where t1.valid=1";

        List<Entity> query = db.query(sql);
        for (Entity entity : query) {
            ProductConstituentsInfo info = new ProductConstituentsInfo();
            info.setTradeDay(entity.getInt("trade_day"));
            info.setLastTradeDay(entity.getInt("last_trade_day"));
            info.setProductCode(entity.getStr("product_code"));
            info.setProductName(entity.getStr("product_name"));
            info.setAdjMktCap(entity.getBigDecimal("adj_mkt_cap"));
            info.setLastAdjMktCap(entity.getBigDecimal("last_adj_mkt_cap"));
            info.setDivisor(entity.getBigDecimal("divisor"));
            info.setLastDivisor(entity.getBigDecimal("last_divisor"));
            info.setClosePrice(entity.getBigDecimal("close_price"));
            info.setValid(entity.getShort("valid"));
            String stockCode = entity.getStr("stock_code");
            int adjShare = entity.getInt("adj_share");
            info.setStockCode(stockCode);
            info.setAdjShare(adjShare);
            constituents.add(info);
        }
        source.close();
        ctx.collect(constituents);
    }

    @Override
    public void cancel() {

    }
}
