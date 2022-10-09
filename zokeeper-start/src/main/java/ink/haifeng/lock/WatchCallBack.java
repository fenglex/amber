package ink.haifeng.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WatchCallBack implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {

    ZooKeeper zk;
    String threadName;

    String pathName;
    CountDownLatch latch = new CountDownLatch(1);

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void process(WatchedEvent event) {
        //如果第一个哥们，那个锁释放了，其实只有第二个收到了回调事件！！
        //如果，不是第一个哥们，某一个，挂了，也能造成他后边的收到这个通知，从而让他后边那个跟去watch挂掉这个哥们前边的。。。
        switch (event.getType()) {
            case NodeDeleted:
                zk.getChildren("/", false, this, "ctx");
            default:
                // pass
        }
    }

    public void tryLock() {
        try {
            System.out.println(threadName + "  create .....");
            zk.create("/lock", threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, this, "ct2x");
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void unlock() {
        try {
            System.out.println(threadName+" over working ....");
            zk.delete(pathName,-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }


    // ChildrenCallback
//    public void processResult(int i, String s, Object o, List<String> list) {
//        Collections.sort(list);
//        int index = list.indexOf(pathName.substring(1));
//        if (i == 0) {
//            System.out.println(threadName + " is first");
//            try {
//                zk.setData("/", threadName.getBytes(), -1);
//                latch.countDown();
//            } catch (KeeperException e) {
//                throw new RuntimeException(e);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        } else {
//            zk.exists("/" + list.get(i - 1), this, this, "ctx");
//        }//   }

    // 创建节点回调
    public void processResult(int i, String s, Object o, String s1) {
        if (s1 != null) {
            System.out.println(threadName + " create node:" + s1);
            pathName = s1;
            // this 调用 ChildrenCallback
            zk.getChildren("/", false, this, "ct1x");
        }
    }

    //   状态回调
    public void processResult(int i, String s, Object o, Stat stat) {
         // pass
    }

    public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
        Collections.sort(list);
        int index = list.indexOf(pathName.substring(1));
        if (i == 0) {
            System.out.println(threadName + " is first");
            try {
                zk.setData("/", threadName.getBytes(), -1);
                latch.countDown();
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            zk.exists("/" + list.get(i - 1), this, this, "ct1x");
        }
    }
}
