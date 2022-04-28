package ink.haifeng.quotation.common;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 13:48:35
 */
public class TimeUtil {

    /**
     * 判断这个分钟是否是交易时间
     *
     * @param minute
     */
    public static boolean isTradeTime(int minute) {
        if (minute == Constants.MINUTE_9_25) {
            return true;
        } else if (minute >= Constants.MINUTE_9_30 && minute <= Constants.MINUTE_11_29) {
            return true;
        } else if (minute >= Constants.MINUTE_13_00 && minute <= Constants.MINUTE_14_57) {
            return true;
        } else {
            return minute == Constants.MINUTE_15_00;
        }
    }
}
