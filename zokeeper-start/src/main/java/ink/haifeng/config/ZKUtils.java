package ink.haifeng.config;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKUtils {
    protected static ZooKeeper zk;
    private static String address = "172.16.1.13:2181/testConfig";

    private static DefaultWatch watch = new DefaultWatch();

    private static CountDownLatch countDownLatch=new CountDownLatch(1);
    public static ZooKeeper getZk() {
        try {
            zk=new ZooKeeper(address,1000,watch);
            watch.setDownLatch(countDownLatch);
            countDownLatch.await();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return zk;
    }
}
