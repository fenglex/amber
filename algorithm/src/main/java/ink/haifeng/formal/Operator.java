package ink.haifeng.formal;

/**
 * Operator
 * 加减乘除的操作
 *
 * @author haifeng
 * @since 1.0
 */
public class Operator {

    /**
     * 加
     * @param a
     * @param b
     * @return
     */
    public static int add(int a, int b) {
        while (b != 0) {
            int tmp = a ^ b;
            b = (a & b) << 1;
            a = tmp;
        }
        return a;
    }

    public static int sub(int a, int b) {
        return add(a, add(~b, 1));
    }


    public static int multi(int a, int b) {
        int sum = 0;
        while (b != 0) {
            if ((b & 1) == 1) {
                sum = add(sum, a);
            }
            b = b >>> 1;
            a = a << 1;
        }
        return sum;
    }


    public static int negNum(int n) {
        //转化为相反数
        return add(~n, 1);
    }

    public static boolean isNeg(int n) {
        //判断是否为负数
        return n < 0;
    }

    public static int div(int a, int b) {
        int x = isNeg(a) ? negNum(a) : a;
        int y = isNeg(b) ? negNum(b) : b;
        int res = 0;
        for (int i = 31; i >= 0; i = sub(i, 1)) {
            if ((x >> i) >= y) {
                res |= (1 << i);
                x = sub(x, y << i);
            }
        }
        return isNeg(a) != isNeg(b) ? negNum(res) : res;
    }

    public static int divide(int a, int b) {
        if (a == Integer.MIN_VALUE && b == Integer.MIN_VALUE) {
            return 1;
        } else if (b == Integer.MIN_VALUE) {
            return 0;
        } else if (a == Integer.MIN_VALUE) {
            if (b == negNum(1)) {
                return Integer.MAX_VALUE;
            } else {
                int c = div(add(a, 1), b);
                return add(c, div(sub(a, multi(c, b)), b));
            }
        } else {
            return div(a, b);
        }
    }


    public static void main(String[] args) {
        System.out.println(add(10, 23));
        System.out.println(sub(10, 23));
        System.out.println(multi(10, 23));
        System.out.println(divide(20, 2));
    }
}
