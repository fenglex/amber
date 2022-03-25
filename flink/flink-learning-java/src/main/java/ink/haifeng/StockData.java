package ink.haifeng;


import java.math.BigDecimal;

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

    public StockData() {
    }

    public StockData(String line) {
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
    }

    public String getStockCode() {
        return stockCode;
    }

    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }

    public String getTradeDay() {
        return tradeDay;
    }

    public void setTradeDay(String tradeDay) {
        this.tradeDay = tradeDay;
    }

    public int getRealtime() {
        return realtime;
    }

    public void setRealtime(int realtime) {
        this.realtime = realtime;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public BigDecimal getPreClose() {
        return preClose;
    }

    public void setPreClose(BigDecimal preClose) {
        this.preClose = preClose;
    }

    public BigDecimal getOpen() {
        return open;
    }

    public void setOpen(BigDecimal open) {
        this.open = open;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public void setHigh(BigDecimal high) {
        this.high = high;
    }

    public BigDecimal getLow() {
        return low;
    }

    public void setLow(BigDecimal low) {
        this.low = low;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getVolume() {
        return volume;
    }

    public void setVolume(BigDecimal volume) {
        this.volume = volume;
    }
}


