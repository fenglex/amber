package ink.haifeng.jetty;

/**
 * RunOnExit
 *
 * @author haifeng
 * @version 2023/1/30 10:54
 */
public class RunOnExit {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("程序启动了");
        // 此段代码的位置关系到是否执行
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("程序结束了");
        }));
        Thread.sleep(1000 * 60);
        System.exit(0);
    }
}
