package ink.haifeng.quotation.source;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.model.dto.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 17:58:14
 */
public class ProductInfoSource extends RichSourceFunction<ProductBroadcastData> {
    private ParameterTool params;
    private SimpleDataSource source;
    private Db db;

    private final String format = "yyyyMMdd";

    private final Set<String> configDays = new HashSet<>(5000);

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
    public void run(SourceContext<ProductBroadcastData> ctx) throws Exception {

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("product-info-schedule-pool-%d").daemon(false).build());
        executorService.scheduleAtFixedRate(() -> {
            String runDay = getCurrentRunDay();
            if (!configDays.contains(runDay)) {
                updateProductInfos(ctx);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }


    private void updateProductInfos(SourceContext<ProductBroadcastData> ctx) {
        String runDay = getCurrentRunDay();
        initDataSource();
        List<ProductBasicInfo> basicInfos = basicInfos(runDay);
        List<ProductStockConstituents> constituents = productStockConstituents(runDay);
        Map<String, ProductPriceLowHigh> priceLowHighMap = fiveAndTwoYearHighLow(basicInfos);
        List<ProductInfo> dataList = new ArrayList<>(5000);
        Set<String> stockList =
                constituents.stream().map(ProductStockConstituents::getStockCode).collect(Collectors.toSet());
        Map<String, BigDecimal> stockProCloseMap = stockProClose(stockList, runDay);
        for (ProductBasicInfo basicInfo : basicInfos) {
            String productCode = basicInfo.getProductCode();
            ProductInfo data = new ProductInfo();
            data.setBasicInfo(basicInfo);
            for (ProductStockConstituents constituent : constituents) {
                if (constituent.getProductCode().equals(productCode)) {
                    String stockCode = constituent.getStockCode();
                    constituent.setPreClose(stockProCloseMap.get(stockCode));
                    data.getConstituents().add(constituent);
                }
            }
            ProductPriceLowHigh lowHighPrice = priceLowHighMap.getOrDefault(productCode, new ProductPriceLowHigh());
            data.setPriceLowHigh(lowHighPrice);
            if (String.valueOf(data.getBasicInfo().getTradeDay()).equals(runDay)) {
                dataList.add(data);
            }
        }
        closeDataSource();
        if (!basicInfos.isEmpty() && !constituents.isEmpty()) {
            ProductBroadcastData broadcastData = new ProductBroadcastData(Integer.parseInt(runDay),dataList);
            ctx.collect(broadcastData);
            configDays.add(runDay);
        } else {
            int dayOfWeek = DateUtil.dayOfWeek(new Date());
            if (dayOfWeek != 1 && dayOfWeek != 6) {
                String minute = DateUtil.format(new Date(), "Hmm");
                if (Integer.parseInt(minute) > 910) {
                    // TODO 当日basic info 为空 请进行检查
                    System.out.println("basic info 为空");
                }
            }
        }
    }

    @Override
    public void cancel() {
        source.close();
    }


    private List<ProductBasicInfo> basicInfos(String runDay) {
        List<ProductBasicInfo> infos = new ArrayList<>(50000);
        String sql = "SELECT  `trade_day`,`last_trade_day`,`product_code`,`product_name`,`adj_mkt_cap`," +
                "`last_adj_mkt_cap`,`divisor`,`last_divisor`,`close_price`,`valid` FROM `tb_product_index_basicinfo`" +
                " where valid=1 and trade_day=%s";

        List<Entity> query = null;
        try {
            query = db.query(String.format(sql, runDay));
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
    public List<ProductStockConstituents> productStockConstituents(String runDay) {
        String sql = "SELECT product_code,stock_code,adj_share FROM tb_product_index_constituents where trade_day=%s";
        List<ProductStockConstituents> constituents = new ArrayList<>();
        List<Entity> entities = null;
        try {
            entities = db.query(String.format(sql, runDay));
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


    private Map<String, BigDecimal> stockProClose(Set<String> stockList, String tradeDay) {
        String sql = "SELECT close_price from tb_stock_eod " +
                "where stock_code='%s' and trade_day<'%s' " +
                "order by trade_day desc limit 1";
        Map<String, BigDecimal> preCloseMap = new HashMap<>(5000);
        for (String stockCode : stockList) {
            List<Entity> entities = null;
            try {
                entities = db.query(String.format(sql, stockCode, tradeDay));
                for (Entity entity : entities) {
                    BigDecimal closePrice = entity.getBigDecimal("close_price");
                    if (closePrice != null) {
                        preCloseMap.put(stockCode, closePrice);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return preCloseMap;
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
        String runDay = params.get(Constants.RUN_DAY, "");
        if (StrUtil.isEmpty(runDay)) {
            runDay = DateUtil.format(new Date(), format);
        }
        return runDay;
    }

    private void sendMail(String content) {

    }
}
