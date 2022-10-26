package ink.haifeng;

public class RandToRand {
    public static void main(String[] args) {
        int[] counts = new int[8];
        int repeat = 10000000;
        for (int i = 0; i < repeat; i++) {
            int num = f3();
            counts[num]++;
        }
        for (int i = 0; i < 8; i++) {
            System.out.println(i + "出现了：" + counts[i] + ",概率->" + ((double) counts[i] / (double) repeat));
        }

        System.out.println("=======================");
        counts = new int[8];
        for (int i = 0; i < repeat; i++) {
            int num = f4();
            counts[num]++;
        }
        for (int i = 0; i < 8; i++) {
            System.out.println(i + "出现了：" + counts[i] + ",概率->" + ((double) counts[i] / (double) repeat));
        }

    }


    /**
     * 返回[0,1) 的一个小数
     * 任意的x，x属于[0,1) ,[0,1)范围上的输出线的概率由原来的x调整为x的平方
     *
     * @return
     */
    public static double xToXPower2() {
        return Math.max(Math.random(), Math.random());
    }

    /**
     * 返回[0,1) 的一个小数
     * 任意的x，x属于[0,1) ,[0,1)范围上的输出线的概率由原来的x调整为1-(1-x)^
     *
     * @return
     */
    public static double xToXPower2Min() {
        return Math.min(Math.random(), Math.random());
    }


    /**
     * 等概率生成1~5的值
     *
     * @return .
     */
    public static int f1() {
        return (int) (Math.random() * 5) + 1;
    }

    /**
     * 仅仅使用f1
     * 等概率返回0,1
     *
     * @return 。
     */
    public static int f2() {
        int ans = 0;
        do {
            ans = f1();
        } while (ans == 3);
        return ans < 3 ? 0 : 1;
    }


    /**
     * 等概率返回0-7
     *
     * @return
     */
    public static int f3() {
        return (f2() << 2) + (f2() << 1) + f2();
    }

    /**
     * 等概率生成1-7
     *
     * @return
     */
    public static int f4() {
        int ans;
        do {
            ans = f4();
        } while (ans == 0);
        return ans;
    }

    /**
     * 固定概率返回0和1 （并不是等概率返回）
     *
     * @return .
     */
    public static int x() {
        return Math.random() < 0.84 ? 0 : 1;
    }

    /**
     * 等概率返回0和1
     *
     * @return
     */
    public static int y() {
        int ans = 0;
        do {
            ans = x();
        } while (ans == x());
        return ans;
    }
}
