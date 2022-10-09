package ink.haifeng.config;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.*;

public class TestConfig {
    ZooKeeper zk;

    @BeforeEach
    public void conn() {
        zk = ZKUtils.getZk();
    }

//    @AfterEach
//    public void close(){
//        try {
//            zk.close();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }


    @Test
    public void getConf(){
        WatchCallBack watchCallBack = new WatchCallBack();
        watchCallBack.setZk(zk);
        MyConf conf = new MyConf();
        watchCallBack.setConf(conf);
        watchCallBack.await();

        while (true){
            if (conf.getConf().equals("")){
                System.out.println("配置为空： 。。。。。。。");
                watchCallBack.await();
            }else {
                System.out.println(conf.getConf());
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
