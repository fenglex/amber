package ink.haifeng.quotation;

import cn.hutool.core.date.DateUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 10:16:30
 */
public class TimerTest {
    public static void main(String[] args) {


        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("example-schedule-pool-%d").daemon(false).build());
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("run job->" + DateUtil.formatDateTime(new Date()));
               // executorService.shutdown();
            }
        }, 0, 5, TimeUnit.SECONDS);

    }
}
