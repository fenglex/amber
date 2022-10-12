package ink.haifeng;

import cn.hutool.core.util.RandomUtil;

public class Test {
    public static void main(String[] args) {
//        int maxValue = 10;
//        int[] arr = new int[20];
//        for (int i = 0; i < 100000; i++) {
//            //int ran = (int) (Math.random() * (10+1));
//            int ran = (int) ((maxValue + 1) * Math.random()) - (int) (maxValue * Math.random());
//            arr[ran] = arr[ran] + 1;
//        }
//        for (int i = 0; i < arr.length; i++) {
//            System.out.println(i + " \t " + arr[i] + +(double) (arr[i] / 100000));
//        }

        printArr(generateRandomArray(10, 10));

    }


    public static void printArr(int[] arr) {
        for (int i : arr) {
            System.out.print(i + ",");
        }
        System.out.println();
    }

    public static int[] generateRandomArray(int maxSize, int maxValue) {
        int[] arr = new int[(int) ((maxSize + 1) * Math.random())];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (int) ((maxValue + 1) * Math.random()) - (int) (maxValue * Math.random());
        }
        return arr;
    }
}
