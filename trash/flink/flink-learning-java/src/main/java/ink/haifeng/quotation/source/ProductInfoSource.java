package ink.haifeng.quotation.source;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.ThreadUtil;
import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.model.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/27 17:58:14
 */

@Slf4j
public class ProductInfoSource extends RichSourceFunction<BasicInfoData> {

    private final Set<String> configDays = new HashSet<>(5000);

    private Connection connection;


    private Boolean isRunning;

    @Override
    public void open(Configuration parameters) throws Exception {
        isRunning = true;
    }

    @Override
    public void run(SourceContext<BasicInfoData> ctx) throws Exception {
        boolean first = true;
        while (isRunning) {
            try {
                String runDay = getCurrentRunDay();
                int minute = Integer.parseInt(DateUtil.format(new Date(), "Hmm"));
                if (!configDays.contains(runDay)) {
                    log.info("开始初始化当日basic_info数据:" + runDay);
                    if (first) {
                        BasicInfoData data = updateProductInfos(runDay);
                        ctx.collect(data);
                        configDays.add(runDay);
                        first = false;
                        log.info("结束初始化当日basic_info数据:" + runDay);
                    } else if (minute > 910) {
                        BasicInfoData data = updateProductInfos(runDay);
                        if (data.getInfos().isEmpty()) {
                            int dayOfWeek = DateUtil.dayOfWeek(new Date());
                            if (dayOfWeek != 1 && dayOfWeek != 6) {
                                // TODO 当日basic info 为空 请进行检查
                                System.out.println("basic info 为空");
                            }
                        } else {
                            ctx.collect(data);
                            configDays.add(runDay);
                            log.info("结束初始化当日basic_info数据:" + runDay);
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                log.error("初始化basic_info数据失败");
            }
            ThreadUtil.sleep(5, TimeUnit.MINUTES);
        }
    }


    private BasicInfoData updateProductInfos(String tradeDay) throws SQLException {
        initConnection();
        List<ProductBasicInfo> basicInfos = basicInfos(tradeDay);
        log.info("获取basic—info数据结束,总计{}", basicInfos.size());
        List<ProductConstituents> constituents = productStockConstituents(tradeDay);
        log.info("获取constituents数据结束,总计{}", constituents.size());
        // TODO remove
        // Map<String, ProductLowHighPrice> priceLowHighMap = fiveAndTwoYearHighLow(basicInfos, tradeDay);
        Map<String, ProductLowHighPrice> priceLowHighMap = new HashMap<>();
        log.info("获取历史最高最低价数据结束,总计{}", priceLowHighMap.size());
        Set<String> stockList =
                constituents.stream().map(ProductConstituents::getStockCode).collect(Collectors.toSet());
        // TODO remove
        //Map<String, StockPreClosePrice> stockProCloseMap = stockProClose(stockList, tradeDay);
        Map<String, StockPreClosePrice> stockProCloseMap = new HashMap<>();
        log.info("获取个股历史收盘价,总计{}", stockProCloseMap.size());
        BasicInfoData basicInfoData = new BasicInfoData();
        List<ProductInfo> infos = new ArrayList<>(5000);
        basicInfoData.setTradeDay(Integer.parseInt(tradeDay));
        for (ProductBasicInfo basicInfo : basicInfos) {
            ProductInfo productInfo = new ProductInfo(basicInfo.getProductCode());
            productInfo.setBasicInfo(basicInfo);
            String productCode = basicInfo.getProductCode();
            ProductLowHighPrice lowHighPrice = priceLowHighMap.getOrDefault(productCode, new ProductLowHighPrice());
            productInfo.setLowHighPrice(lowHighPrice);
            for (ProductConstituents constituent : constituents) {
                if (constituent.getProductCode().equals(productCode)) {
                    String stockCode = constituent.getStockCode();
                    StockPreClosePrice stockPreClosePrice = stockProCloseMap.get(stockCode);
                    productInfo.getConstituents().put(constituent.getStockCode(), constituent);
                    productInfo.getStockPreClose().put(stockCode, stockPreClosePrice == null ?
                            new StockPreClosePrice() : stockPreClosePrice);
                }
            }
            infos.add(productInfo);
        }
        basicInfoData.setInfos(infos);
        closeConnection();
        return basicInfoData;
    }

    @Override
    public void cancel() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        isRunning = false;
    }


    private List<ProductBasicInfo> basicInfos(String runDay) throws SQLException {
        List<ProductBasicInfo> infos = new ArrayList<>(50000);
        String sql = "SELECT  `trade_day`,`last_trade_day`,`product_code`,`product_name`,`adj_mkt_cap`," +
                "`last_adj_mkt_cap`,`divisor`,`last_divisor`,`close_price`,`valid` FROM `tb_product_index_basicinfo`" + " where valid=1 and trade_day=%s";
        Statement statement = connection.createStatement();
        sql = String.format(sql, runDay);
        log.info("exec sql:" + sql);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            ProductBasicInfo basicInfo = new ProductBasicInfo();
            basicInfo.setTradeDay(resultSet.getInt("trade_day"));
            basicInfo.setLastTradeDay(resultSet.getInt("last_trade_day"));
            basicInfo.setProductCode(resultSet.getString("product_code"));
            basicInfo.setProductName(resultSet.getString("product_name"));
            basicInfo.setAdjMktCap(resultSet.getBigDecimal("adj_mkt_cap"));
            basicInfo.setLastAdjMktCap(resultSet.getBigDecimal("last_adj_mkt_cap"));
            basicInfo.setDivisor(resultSet.getBigDecimal("divisor"));
            basicInfo.setLastDivisor(resultSet.getBigDecimal("last_divisor"));
            basicInfo.setClosePrice(resultSet.getBigDecimal("close_price"));
            basicInfo.setValid((short) 1);
            infos.add(basicInfo);
        }
        statement.close();
        return infos;
    }

    /**
     * @return
     */
    public List<ProductConstituents> productStockConstituents(String runDay) throws SQLException {
        String sql = "SELECT product_code,stock_code,adj_share FROM tb_product_index_constituents where trade_day=%s";
        List<ProductConstituents> constituents = new ArrayList<>();
        Statement statement = connection.createStatement();
        String execSql = String.format(sql, runDay);
        log.info("exec sql:" + execSql);
        ResultSet resultSet = statement.executeQuery(execSql);
        while (resultSet.next()) {
            ProductConstituents stockConstituents = new ProductConstituents();
            stockConstituents.setProductCode(resultSet.getString("product_code"));
            stockConstituents.setStockCode(resultSet.getString("stock_code"));
            stockConstituents.setAdjShare(resultSet.getInt("adj_share"));
            constituents.add(stockConstituents);
        }
        statement.close();
        return constituents;
    }


    private Map<String, StockPreClosePrice> stockProClose(Set<String> stockList, String tradeDay) throws SQLException {
        String sql =
                "SELECT close_price,trade_day from tb_stock_eod " + "where stock_code='%s' and trade_day<'%s' " +
                        "order by trade_day desc limit 1";
        Map<String, StockPreClosePrice> preCloseMap = new HashMap<>(5000);
        Statement statement = connection.createStatement();
        for (String stockCode : stockList) {
            String execSql = String.format(sql, stockCode, tradeDay);
            // log.info("exec sql:" + execSql);
            ResultSet resultSet = statement.executeQuery(execSql);
            while (resultSet.next()) {
                BigDecimal closePrice = resultSet.getBigDecimal("close_price");
                int day = resultSet.getInt("trade_day");
                if (closePrice != null) {
                    preCloseMap.put(stockCode, new StockPreClosePrice(stockCode, day, closePrice));
                }
            }
        }
        statement.close();
        return preCloseMap;
    }

    /**
     * 获取两年和五年的产品最高价和最低价
     *
     * @param basicInfos
     * @return
     */
    private Map<String, ProductLowHighPrice> fiveAndTwoYearHighLow(List<ProductBasicInfo> basicInfos,
                                                                   String tradeDay) throws SQLException {
        String format = "yyyyMMdd";
        String twoYearAgo = DateUtil.format(DateUtil.offset(DateUtil.parse(tradeDay, format), DateField.YEAR, -2),
                format);
        String fiveYearAgo = DateUtil.format(DateUtil.offset(DateUtil.parse(tradeDay, format), DateField.YEAR, -2),
                format);
        String sql =
                "SELECT  product_code," +
                        " IF(MAX(high_price) > MAX(close_price),MAX(high_price), MAX(close_price)) high_price," +
                        " IF(MIN(low_price) < MIN(close_price),MIN(low_price), MIN" + "(close_price)) low_price " +
                        "FROM tb_product_index_eod " +
                        "WHERE  trade_day > %s AND trade_day <%s  AND product_code = '%s'";

        Map<String, ProductLowHighPrice> productPriceLowHighMap = new HashMap<>(5000);
        Statement statement = connection.createStatement();
        for (ProductBasicInfo basicInfo : basicInfos) {
            ProductLowHighPrice priceLowHigh = new ProductLowHighPrice();
            String productCode = basicInfo.getProductCode();
            ResultSet resultSet = statement.executeQuery(String.format(sql, twoYearAgo, tradeDay, productCode));
            while (resultSet.next()) {
                priceLowHigh.setTwoYearHigh(resultSet.getBigDecimal("high_price"));
                priceLowHigh.setTwoYearLow(resultSet.getBigDecimal("low_price"));
            }
            resultSet = statement.executeQuery(String.format(sql, fiveYearAgo, tradeDay, productCode));
            while (resultSet.next()) {
                priceLowHigh.setFiveYearLow(resultSet.getBigDecimal("low_price"));
                priceLowHigh.setFiveYearHigh(resultSet.getBigDecimal("high_price"));
            }
            productPriceLowHighMap.put(productCode, priceLowHigh);
        }
        statement.close();
        return productPriceLowHighMap;
    }

    private String getCurrentRunDay() {
        ExecutionConfig.GlobalJobParameters parameters =
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();
        return map.getOrDefault(Constants.RUN_DAY, DateUtil.format(new Date(), Constants.DATE_FORMAT));
    }


    /**
     * 初始化数据库连接
     */
    private void initConnection() {
        Map<String, String> map = this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        try {
            connection = DriverManager.getConnection(map.get("jdbc.url"), map.get("jdbc.user"), map.get("jdbc" +
                    ".password"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void closeConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendMail(String content) {

    }
}
