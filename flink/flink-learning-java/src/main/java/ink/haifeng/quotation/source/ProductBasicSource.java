package ink.haifeng.quotation.source;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import ink.haifeng.quotation.model.entity.ProductIndexBasicInfo;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 17:54:21
 */
public class ProductBasicSource extends RichSourceFunction<ProductIndexBasicInfo> {


    private ParameterTool params;
    private int currentDay;

    @Override
    public void open(Configuration parameters) throws Exception {
        ExecutionConfig.GlobalJobParameters parameter =
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        params = (ParameterTool) parameter;

    }

    @Override
    public void run(SourceContext<ProductIndexBasicInfo> ctx) throws Exception {
        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("basic-info-source-%d").daemon(true).build());
        executor.scheduleAtFixedRate(() -> {
            try {
                List<ProductIndexBasicInfo> basicInfos = loadData(params);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.MINUTES);
    }

    @Override
    public void cancel() {

    }



    private List<ProductIndexBasicInfo> loadData(ParameterTool params) throws SQLException {
        SimpleDataSource source = new SimpleDataSource(params.get("quotation-url"), params.get("quotation-user"),
                params.get("quotation-pass"));
        Db db = Db.use(source);
        String basicInfoSql = "SELECT trade_day,last_trade_day,product_code,product_name,adj_mkt_cap," +
                "last_adj_mkt_cap,divisor," +
                "last_divisor,close_price,valid FROM tb_product_index_basicinfo where valid=1";
        List<Entity> query = db.query(basicInfoSql);
        List<ProductIndexBasicInfo> basicInfos = new ArrayList<>(5000);
        for (Entity entity : query) {
            ProductIndexBasicInfo basicInfo = new ProductIndexBasicInfo();
            basicInfo.setTradeDay(entity.getInt("trade_day"));
            basicInfo.setLastTradeDay(entity.getInt("last_trade_day"));
            basicInfo.setProductCode(entity.getStr("product_code"));
            basicInfo.setProductName(entity.getStr("product_name"));
            basicInfo.setAdjMktCap(entity.getBigDecimal("adj_mkt_cap"));
            basicInfo.setLastAdjMktCap(entity.getBigDecimal("last_adj_mkt_cap"));
            basicInfo.setDivisor(entity.getBigDecimal("divisor"));
            basicInfo.setLastDivisor(entity.getBigDecimal("last_divisor"));
            basicInfo.setClosePrice(entity.getBigDecimal("close_price"));
            basicInfo.setValid(entity.getShort("valid"));
            basicInfos.add(basicInfo);
        }
        return basicInfos;
    }

}
