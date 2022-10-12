package ink.haifeng;

import cn.hutool.core.util.RandomUtil;

public class SepFind {
    public static void main(String[] args) {
        int[] arr = Sort.lenRandomValueRandom(20, 10);
        int[] ints = Sort.bubbleSort(arr);
        for (int anInt : ints) {
            System.out.print(anInt + ",");
        }
        System.out.println();
        boolean b = find(ints, 4);
        System.out.println(b);
    }

    public static boolean find(int[] arr, int num) {
        if (arr == null || arr.length == 0) {
            return false;
        }
        int l = 0, r = arr.length - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (arr[mid] == num) {
                return true;
            } else if (arr[mid] < num) {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return false;
    }

    // 对数器
    public static boolean test(int[] sortedArr, int num) {
        for (int cur : sortedArr) {
            if (cur == num) {
                return true;
            }
        }
        return false;
    }
}
