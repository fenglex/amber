package ink.haifeng.quotation.common;

import cn.hutool.crypto.SecureUtil;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/26 18:05:33
 */
public class Constants {

    public static final int MINUTE_11_29 = 1129;
    public static final int MINUTE_15_00 = 1500;
    public static final int MINUTE_9_25 = 925;
    public static final int MINUTE_9_30 = 930;
    public static final int MINUTE_13_00 = 1300;
    public static final int MINUTE_14_57 = 1457;


    public static final String RUN_DAY = "run.day";

    public static final String STOCK_REDIS_KEY = String
            .format("%s%s%s", "69", "901", SecureUtil.md5("").substring(8, 24));
    public static final String PRODUCT_REDIS_KEY = String
            .format("%s%s%s", "69", "902", SecureUtil.md5("").substring(8, 24));
}
