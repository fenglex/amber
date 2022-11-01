package ink.haifeng.formal;

import java.util.HashSet;

public class BitMap {
    private int[] bits;

    public BitMap(int max) {
        bits = new int[(max / 32) + 1];
    }

    public void add(int num) {
        // bits[num >> 5] |= (1 << (num & 31));
        int idx = num / 32;
        int pos = num % 32;
        bits[idx] = bits[idx] | (1 << pos);
    }

    public boolean contains(int num) {
        // return (bits[num >> 5] & (1 << (num & 31))) != 0;
        int idx = num / 32;
        int pos = num % 32;
        return (bits[idx] & (1 << pos)) != 0;
    }


    public void delete(int num) {
        // bits[num >> 5] &= ~(1 << (num & 31));
        int idx = num / 32;
        int pos = num % 32;
        bits[idx] = bits[idx] & (~(1 << pos));
    }

    /*
     *打印一个数的二进制码
     */
    public static void printBinary(int a) {
        for (int i = 31; i >= 0; i--) {
            System.out.print(((a >> i) & 1));
        }
    }

    public static void main(String[] args) {
        System.out.println("测试开始！");
        int max = 10000;
        BitMap bitMap = new BitMap(max);
        HashSet<Integer> set = new HashSet<>();
        int testTime = 10000000;
        for (int i = 0; i < testTime; i++) {
            int num = (int) (Math.random() * (max + 1));
            double decide = Math.random();
            if (decide < 0.333) {
                bitMap.add(num);
                set.add(num);
            } else if (decide < 0.666) {
                bitMap.delete(num);
                set.remove(num);
            } else {
                if (bitMap.contains(num) != set.contains(num)) {
                    System.out.println("Oops! 1");
                    break;
                }
            }
        }
        for (int num = 0; num <= max; num++) {
            if (bitMap.contains(num) != set.contains(num)) {
                System.out.println("Oops! 2");
            }
        }
        System.out.println("测试结束！");
    }


}
