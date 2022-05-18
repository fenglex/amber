package ink.haifeng.quotation.source;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import ink.haifeng.quotation.model.dto.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 17:58:14
 */
public class ProductInfoSource extends RichSourceFunction<List<ProductDimensionData>> {
    private ParameterTool params;
    private SimpleDataSource source;
    private Db db;

    private final String format = "yyyyMMdd";

    /**
     * 初始化数据库连接
     */
    private void initDataSource() {
        params = (ParameterTool) this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        source = new SimpleDataSource(params.get("jdbc.url"), params.get("jdbc.user"), params.get("jdbc.password"));
        db = Db.use(source);
    }

    private void closeDataSource() {
        source.close();
    }

    @Override
    public void run(SourceContext<List<ProductDimensionData>> ctx) throws Exception {
        updateProductInfos(ctx);
    }


    private void updateProductInfos(SourceContext<List<ProductDimensionData>> ctx){
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("product-info-schedule-pool-%d").daemon(false).build());
        executorService.scheduleAtFixedRate(() -> {
            initDataSource();
            List<ProductBasicInfo> basicInfos = basicInfos();
            List<ProductStockConstituents> constituents = productStockConstituents();
            Map<String, ProductPriceLowHigh> priceLowHighMap = fiveAndTwoYearHighLow(basicInfos);
            List<ProductDimensionData> dataList = new ArrayList<>();
            String runDay = getCurrentRunDay();
            for (ProductBasicInfo basicInfo : basicInfos) {
                String productCode = basicInfo.getProductCode();
                ProductDimensionData data = new ProductDimensionData(basicInfo);
                for (ProductStockConstituents constituent : constituents) {
                    if (constituent.getProductCode().equals(productCode)) {
                        data.getConstituents().add(constituent);
                    }
                }
                ProductPriceLowHigh lowHighPrice = priceLowHighMap.getOrDefault(productCode, new ProductPriceLowHigh());
                data.setTwoYearHigh(lowHighPrice.getTwoYearHigh());
                data.setTwoYearLow(lowHighPrice.getTwoYearLow());
                data.setFiveYearHigh(lowHighPrice.getFiveYearHigh());
                data.setFiveYearLow(lowHighPrice.getFiveYearLow());
                if (String.valueOf(data.getTradeDay()).equals(runDay)) {
                    dataList.add(data);
                }
            }
            closeDataSource();
            ctx.collect(dataList);
        }, 0, 5, TimeUnit.SECONDS);
    }
    @Override
    public void cancel() {
        source.close();
    }


    private List<ProductBasicInfo> basicInfos() {
        List<ProductBasicInfo> infos = new ArrayList<>(50000);
        String sql = "SELECT \n" + "    `trade_day`,\n" + "    `last_trade_day`,\n" + "    `product_code`,\n" + "    " +
                "`product_name`,\n" + "    `adj_mkt_cap`,\n" + "    `last_adj_mkt_cap`,\n" + "    `divisor`,\n" + "  " +
                "  `last_divisor`,\n" + "    `close_price`,\n" + "    `valid`\n" + "FROM `tb_product_index_basicinfo`" +
                " where valid=1";

        List<Entity> query = null;
        try {
            query = db.query(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        for (Entity entity : query) {
            ProductBasicInfo basicInfo = new ProductBasicInfo();
            basicInfo.setTradeDay(entity.getInt("trade_day"));
            basicInfo.setLastTradeDay(entity.getInt("last_trade_day"));
            basicInfo.setProductCode(entity.getStr("product_code"));
            basicInfo.setProductName(entity.getStr("product_name"));
            basicInfo.setAdjMktCap(entity.getBigDecimal("adj_mkt_cap"));
            basicInfo.setLastAdjMktCap(entity.getBigDecimal("last_adj_mkt_cap"));
            basicInfo.setDivisor(entity.getBigDecimal("divisor"));
            basicInfo.setLastDivisor(entity.getBigDecimal("last_divisor"));
            basicInfo.setClosePrice(entity.getBigDecimal("close_price"));
            basicInfo.setValid((short) 1);
            infos.add(basicInfo);
        }
        return infos;
    }

    /**
     * @return
     */
    public List<ProductStockConstituents> productStockConstituents() {
        String sql = "SELECT product_code,stock_code,adj_share FROM tb_product_index_constituents";
        List<ProductStockConstituents> constituents = new ArrayList<>();
        List<Entity> entities = null;
        try {
            entities = db.query(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        for (Entity entity : entities) {
            ProductStockConstituents stockConstituents = new ProductStockConstituents();
            stockConstituents.setProductCode(entity.getStr("product_code"));
            stockConstituents.setStockCode(entity.getStr("stock_code"));
            stockConstituents.setAdjShare(entity.getInt("adj_share"));
            constituents.add(stockConstituents);
        }
        return constituents;
    }

    /**
     * 获取两年和五年的产品最高价和最低价
     *
     * @param basicInfos
     * @return
     */
    private Map<String, ProductPriceLowHigh> fiveAndTwoYearHighLow(List<ProductBasicInfo> basicInfos) {
        String format = "yyyyMMdd";
        String runDay = getCurrentRunDay();
        String twoYearAgo = DateUtil.format(DateUtil.offset(DateUtil.parse(runDay, format), DateField.YEAR, -2),
                format);
        String fiveYearAgo = DateUtil.format(DateUtil.offset(DateUtil.parse(runDay, format), DateField.YEAR, -2),
                format);
        String sql = "SELECT \n" +
                "    product_code,\n" +
                "    IF(MAX(high_price) > MAX(close_price),\n" +
                "        MAX(high_price),\n" +
                "        MAX(close_price)) high_price,\n" +
                "    IF(MIN(low_price) < MIN(close_price),\n" +
                "        MIN(low_price),\n" +
                "        MIN(close_price)) low_price\n" +
                "FROM\n" +
                "    tb_product_index_eod\n" +
                "WHERE\n" +
                "    trade_day > %s and trade_day <%s \n" +
                "        AND product_code = '%s'\n" +
                "GROUP BY product_code order by trade_day desc limit 1";

        Map<String, ProductPriceLowHigh> productPriceLowHighMap = new HashMap<>(5000);
        for (ProductBasicInfo basicInfo : basicInfos) {
            List<Entity> entities = null;
            ProductPriceLowHigh priceLowHigh = new ProductPriceLowHigh();
            String productCode = basicInfo.getProductCode();
            try {
                entities = db.query(String.format(sql, twoYearAgo, runDay, productCode));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            for (Entity entity : entities) {
                priceLowHigh.setTwoYearHigh(entity.getBigDecimal("high_price"));
                priceLowHigh.setTwoYearLow(entity.getBigDecimal("low_price"));

            }
            try {
                entities = db.query(String.format(sql, fiveYearAgo, runDay, productCode));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            for (Entity entity : entities) {
                priceLowHigh.setFiveYearLow(entity.getBigDecimal("low_price"));
                priceLowHigh.setFiveYearHigh(entity.getBigDecimal("high_price"));
            }
            productPriceLowHighMap.put(productCode, priceLowHigh);
        }
        return productPriceLowHighMap;
    }

    private String getCurrentRunDay() {
        String format = "yyyyMMdd";
        String runDay = params.get("run.day", "");
        if (StrUtil.isEmpty(runDay)) {
            runDay = DateUtil.format(new Date(), format);
        }
        return runDay;
    }

    private void sendMail(String content) {

    }
}
