package ink.haifeng.config;


import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class WatchCallBack implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {


    private ZooKeeper zk;
    private MyConf conf;
    private CountDownLatch countDownLatch = new CountDownLatch(1);


    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public MyConf getConf() {
        return conf;
    }

    public void setConf(MyConf conf) {
        this.conf = conf;
    }


    public void await() {
        zk.exists("/appConf", this, this, "ctx");
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // AsyncCallback.DataCallback
    public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
        if (bytes != null) {
            String data = new String(bytes);
            conf.setConf(data);
            countDownLatch.countDown();
        }
    }

    //  AsyncCallback.StatCallback
    public void processResult(int i, String s, Object o, Stat stat) {
        if (stat != null) {
            zk.getData("/appConf", this, this, "ctx");
        }
    }

    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeCreated:
            case NodeDataChanged:
                zk.getData("/appConf", this, this, "ctx");
                break;
            case NodeDeleted:
                conf.setConf("");
                break;
            default:
        }
    }
}
