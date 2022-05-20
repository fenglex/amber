package ink.haifeng.quotation.model.dto;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import ink.haifeng.common.Constant;
import ink.haifeng.quotation.model.entity.RedisEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;


/**
 * @author fenglex
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Slf4j
public class StockData implements RedisEntity {
    private String stockCode;
    private int tradeDay;
    /**
     * 状态
     * 0 为异常数据
     * -1 解析异常数据
     * -2 为了推动水印生成数据
     */
    private int state = 0;
    private BigDecimal price;
    private BigDecimal preClose;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal open;
    private Long amount;
    private Long volume;
    private String realtime;
    private long timestamp;


    public int minute() {
        return Integer.parseInt(this.realtime) / 100000;
    }

    public StockData(String stockCode, int tradeDay, int state, String price, String preClose, String high, String low,
                     String open, String amount, String volume, String realtime) {
        this.stockCode = stockCode;
        this.tradeDay = tradeDay;
        this.state = state;
        this.price = new BigDecimal(price).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
        this.preClose = new BigDecimal(preClose).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
        this.high = new BigDecimal(high).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
        this.low = new BigDecimal(low).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
        this.open = new BigDecimal(open).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
        this.amount = Long.parseLong(amount);
        this.volume = Long.parseLong(volume);
        this.realtime = realtime;
    }

    public long getTimestamp() {
        if (this.timestamp != 0) {
            return this.timestamp;
        }
        if (this.realtime == null) {
            return 0L;
        } else {
            String minute = realtime.length() == 8 ? "0" + realtime : "" + realtime;
            DateTime time = DateUtil.parse(tradeDay + "" + minute, "yyyyMMddHHmmssSSS");
            return time.getTime();
        }
    }

    public StockData(String line) {
        String[] split = line.split("\001");
        try {
            final String stockCode = split[4];
            final int tradeDay = Integer.parseInt(split[7]);
            final String realtime = split[3];
            final short state = Short.parseShort(split[8]);
            final String preClose = split[9];
            final String openPrice = split[10];
            final String high = split[11];
            final String low = split[12];
            final String price = split[13];
            final String amount = split[20];
            final String volume = split[19];
            this.stockCode = stockCode;
            this.tradeDay = tradeDay;
            this.state = state;
            this.price = new BigDecimal(price).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
            this.preClose = new BigDecimal(preClose).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
            this.high = new BigDecimal(high).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
            this.low = new BigDecimal(low).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
            this.open = new BigDecimal(openPrice).divide(Constant.thousandTen, 4, RoundingMode.FLOOR);
            this.amount = Long.parseLong(amount);
            this.volume = Long.parseLong(volume);
            this.realtime = realtime;
        } catch (Exception e) {
            log.warn("错误的数据(忽略此条数据)->" + line);
            this.state = -1;
        }
    }

    @Override
    public String redisString() {
        BigDecimal zero = new BigDecimal("0");
        BigDecimal gain = this.preClose.compareTo(zero) == 0 ? zero : this.price.setScale(4, RoundingMode.FLOOR)
                .subtract(this.preClose)
                .divide(this.preClose, 6, RoundingMode.HALF_UP)
                .multiply(new BigDecimal(100));
        return this.stockCode + "\001" +
                this.tradeDay + "\001" +
                minute() + "\001" +
                this.price + "\001" +
                this.preClose + "\001" +
                gain;
    }
    public String getCodePrefix() {
        return this.stockCode.substring(0, 6);
    }
}