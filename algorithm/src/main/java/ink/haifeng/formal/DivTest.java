package ink.haifeng.formal;

/**
 * DivTest
 *
 * @author haifeng
 * @since 1.0
 */
public class DivTest {

    public static int add(int a, int b) {
        while (b != 0) {
            int tmp = a ^ b;
            b = (a & b) << 1;
            a = tmp;
        }
        return a;
    }


    public static int minus(int a, int b) {
        return add(a, add(~b, 1));
    }

    public static int multi(int a, int b) {
        int res = 0;
        while (b != 0) {
            res = add(res, a);
            b = b >>> 1;
            a = a << 1;
        }
        return res;
    }

    public static int negNum(int n) {
        //转化为相反数
        return add(~n, 1);
    }

    public static boolean isNeg(int n) {
        //判断是否为负数
        return n < 0;
    }

    /**
     * 两个正整数相除
     *
     * @param a .
     * @param b .
     * @return .
     */
    public static int div(int a, int b) {
        int x = isNeg(a) ? negNum(a) : a;
        int y = isNeg(b) ? negNum(b) : b;
        int res = 0;
        for (int i = 31; i >= 0; i = minus(i, 1)) {
            if ((x >> i) >= y) {
                res |= (1 << i);
                x = minus(x, y << i);
            }
        }
        return isNeg(a) != isNeg(b) ? negNum(res) : res;
    }

    public static int divide(int a, int b) {
        if (a == Integer.MIN_VALUE && b == Integer.MIN_VALUE) {
            return 1;
        } else if (a == Integer.MIN_VALUE) {
            return 0;
        } else if (b == Integer.MIN_VALUE) {
            if (b == negNum(1)) {
                return Integer.MIN_VALUE;
            } else {
                int c = div(add(a, 1), b);
                return add(c, div(minus(a, multi(c, b)), b));
            }
        } else {
            return div(a, b);
        }

    }

    public static void main(String[] args) {
        for (int i = 0; i < Integer.MAX_VALUE/10000; i++) {
            int a =(int)(Math.random() * Integer.MAX_VALUE) + 1;
            int b =(int)(Math.random() * Integer.MAX_VALUE) + 1;
            if (a/b!=divide(a,b)){
                System.out.println("Opps");
            }
        }
        System.out.println("end");
    }
}
