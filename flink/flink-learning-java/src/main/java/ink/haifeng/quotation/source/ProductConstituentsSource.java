package ink.haifeng.quotation.source;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
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
public class ProductConstituentsSource extends RichSourceFunction<ProductIndexConstituents> {
    @Override
    public void run(SourceContext<ProductIndexConstituents> ctx) throws Exception {
        ExecutionConfig.GlobalJobParameters parameters =
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParameterTool params = (ParameterTool) parameters;
        SimpleDataSource source = new SimpleDataSource(params.get("quotation-url"), params.get("quotation-user"),
                params.get("quotation-pass"));
        Db db = Db.use(source);
        List<ProductIndexConstituents> constituents = new ArrayList<>(50000);
        String constituentSql = "select trade_day,product_code,stock_code,adj_share " +
                "from tb_product_index_constituents where stock_code in ('000020.SZ','000059.SZ','000301.SZ')";
        List<Entity> query = db.query(constituentSql);
        for (Entity entity : query) {
            Integer tradeDay = entity.getInt("trade_day");
            String productCode = entity.getStr("product_code");
            String stockCode = entity.getStr("stock_code");
            int adjShare = entity.getInt("adj_share");
            constituents.add(new ProductIndexConstituents(tradeDay, productCode, stockCode, adjShare));
        }
        for (ProductIndexConstituents constituent : constituents) {
            ctx.collect(constituent);
        }
    }

    @Override
    public void cancel() {

    }
}
