package ink.haifeng;


/**
 * 打印整型的32位信息
 */
public class PrintB {


    /**
     * java 整型数值是有1个符号位所以数值范围是 -2^31 ~ 2^31-1
     * <p>
     * 为了统一计算所以java的负数设计为补位形式
     * 如-1的二进制
     * 11111111111111111111111111111111
     * 如何理解：
     * 第一位为符号位，1表示为负数，后面全部取反+1为实际值，取反为0加1后为-1
     * 这种方式主要是为了方便进行统一运算
     *
     * @param args
     */

    public static void main(String[] args) {
        printB(5);
        printB(-1);

        printB(5);
        printB(1<<2);
        // ~ 表示取反，就是将二进制中的0全部转为1,1全部转为0
        printB(~5);

        printB(-5);
        System.out.println((-5 & (1<<2)));
    }

    /**
     * 与运算如5与1<<2 进行与运算
     * 00000000000000000000000000000101 (5)
     * 00000000000000000000000000000100 (1<<2)
     * 00000000000000000000000000000100 (两个数值计算与的结果)
     * @param num
     */
    private static void printB(int num) {
        for (int i = 31; i >= 0; i--) {
            System.out.print((num & (1 << i)) == 0 ? 0 : 1);
        }
        System.out.println();
    }
}
