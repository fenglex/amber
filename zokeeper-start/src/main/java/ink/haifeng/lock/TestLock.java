package ink.haifeng.lock;

import ink.haifeng.config.ZKUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLock {
    ZooKeeper zk;

    @BeforeEach
    public void conn() {
        zk = ZKUtils.getZk();
    }

    @AfterEach
    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLock() {
        for (int i = 0; i < 10; i++) {
            new Thread() {
                @Override
                public void run() {
                    WatchCallBack watchCallBack = new WatchCallBack();
                    watchCallBack.setZk(zk);
                    String threadName = Thread.currentThread().getName();
                    watchCallBack.setThreadName(threadName);
                    watchCallBack.tryLock();
                    System.out.println(threadName+" is working.....");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    watchCallBack.unlock();
                }
            }.start();
        }
        while (true){

        }
    }

}
