package ink.haifeng.dto;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/3/28 18:19:37
 */


public class VisitLog {

    private String region;
    private long timestamp;
    private String dateTime;

    public VisitLog() {

    }

    public VisitLog(String region,  String dateTime) {
        this.region = region;
        DateTime parse = DateUtil.parse(dateTime, "yyyy-MM-dd HH:mm:ss");
        this.timestamp = parse.getTime();
        this.dateTime = dateTime;
    }

    @Override
    public String toString() {
        return "VisitLog{" +
                "region='" + region + '\'' +
                ", timestamp=" + timestamp +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }
}
