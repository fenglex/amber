package ink.haifeng;


import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;

/**
 * @author haifeng
 */
@Getter
@AllArgsConstructor
public class StockData {
    private String stockCode;
    private String tradeDay;
    private int realtime;
    private int state;
    private BigDecimal preClose;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal price;
    private BigDecimal amount;
    private BigDecimal volume;
    private long timestamp;


    public int getMinute() {
        return this.realtime / 100000;
    }


    public StockData(String line) {
        try {
            String[] split = line.split(",");
            this.stockCode = split[0];
            this.tradeDay = split[1];
            this.realtime = Integer.parseInt(split[2]);
            this.state = Integer.parseInt(split[3]);
            this.preClose = new BigDecimal(split[4]);
            this.open = new BigDecimal(split[5]);
            this.high = new BigDecimal(split[6]);
            this.low = new BigDecimal(split[7]);
            this.price = new BigDecimal(split[8]);
            this.amount = new BigDecimal(split[9]);
            this.volume = new BigDecimal(split[10]);
            String minute = String.valueOf(realtime).length() == 8 ? "0" + realtime : "" + realtime;
            DateTime time = DateUtil.parse(tradeDay + "" + minute, "yyyyMMddHHmmssSSS");
            this.timestamp = time.getTime();
        } catch (Exception e) {
            this.state = -1;
        }
    }


}


