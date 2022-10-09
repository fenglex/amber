package ink.haifeng.config;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

public class DefaultWatch implements Watcher {

    private CountDownLatch downLatch;

    public void setDownLatch(CountDownLatch countDownLatch) {
        this.downLatch = countDownLatch;
    }

    public void process(WatchedEvent event) {
        System.out.println(event.toString());
        switch (event.getState()) {
            case SyncConnected:
                System.out.println("zk 连接成功");
                downLatch.countDown();
                break;
            case Closed:
                System.out.println("zk 关闭了");
                break;
            default:
        }

    }
}
