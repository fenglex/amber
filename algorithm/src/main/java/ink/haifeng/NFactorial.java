package ink.haifeng;

/**
 * 计算N的阶乘
 */
public class NFactorial {
    public static void main(String[] args) {
        System.out.println(nFactorial(2));
        System.out.println(nFactorial(3));
    }

    private static long nFactorial(int n) {
        long rs = 0;
        long cur = 1;
        for (int i = 1; i <= n; i++) {
            cur = cur * i;
            rs += cur;
        }
        return rs;
    }
}
