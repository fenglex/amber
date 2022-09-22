package ink.haifeng;

import cn.hutool.core.util.RandomUtil;

/**
 * 常用的排序算法
 */
public class Sort {

    public static void main(String[] args) {
        int[] arr = {16, 6, 9, 6, 12, 17, 2, 15, 12, 7};
        printArr(bubbleSort(arr));
        printArr(selectSort(arr));
        arr = new int[]{16, 6};
        printArr(selectSort(arr));
        arr = new int[]{1};
        printArr(selectSort(arr));
    }

    private static void printArr(int[] arr) {
        for (int i : arr) {
            System.out.print(i + "\t");
        }
        System.out.println();
    }


    /**
     * 交换arr中i,j数的位置
     *
     * @param arr
     * @param i
     * @param j
     */
    private static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    /**
     * 选择排序
     * 1. 先从第0个位置数据到最后一个位置找到最小值放到位置0
     * 2. 先从第1个位置数据到最后一个位置找到最小值放到位置1
     * 3. 先从第2个位置数据到最后一个位置找到最小值放到位置2
     * 重复以上操作直到最后一个位置
     *
     * @param arr
     * @return
     */
    public static int[] selectSort(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            int minIndex = i;
            for (int j = i; j < arr.length; j++) {
                minIndex = arr[minIndex] < arr[j] ? minIndex : j;
            }
            swap(arr, i, minIndex);
        }
        return arr;
    }

    /**
     * 冒泡排序
     * 假设数组长度n
     * 第一遍：
     * 第0位置开始，到n-1的位置
     * 0位置和1位置数进行比较如果0位置数大于1位置数，则两个位置的数相互调换位置，否则不处理
     * 1位置和2位置数进行比较如果1位置数大于2位置数，则两个位置的数相互调换位置，否则不处理
     * 2位置和3位置数进行比较如果2位置数大于3位置数，则两个位置的数相互调换位置，否则不处理
     * 。。
     * 直到最后一个位置前的数据后最后一个数据比较完，这样最大的一个数就在最后一个位置了
     * 第二遍：
     * 第0位置开始到n-2位置
     * 从0~n-2w位置依次比较大小
     * 结束后第二大的数在n-2的位置
     * 重复上面操作
     * ...
     *
     * @param arr
     * @return
     */
    public static int[] bubbleSort(int[] arr) {
        int n = arr.length;
        for (int end = n - 1; end >= 0; end--) {
            for (int second = 1; second <= end; second++) {
                if (arr[second - 1] > arr[second]) {
                    swap(arr, second - 1, second);
                }
            }
        }
        return arr;
    }


    /**
     * 插入排序
     * 假设一个n长度的数组
     * 首先拿到一个数放到数组的0位置，与他前一个位置数据相比较如果小于前一个位置的数据这调换位置（0位置的数前面没有数值，继续下一步）
     * 拿到第二个数值，当到数组1的位置，与他前一个位置数据比较，如果小于它前面的值，调换他们的位置
     * 。。。
     * 拿到第n个数，放到n-1的位置，如果n-1位置数小于n-2位置数调换他们位置，如果n-2位置数据小于n-3位置数据继续调换他们位置，
     * （跳出条件，n-1不再比它前面位置数小的时候，或者n-1前面没有数时，结束当前）
     *
     *
     * @param arr
     * @return
     */
    public static int[] insertSort1(int[] arr) {
        int n = arr.length;
        for (int end = 1; end < n; end++) {
            int newIndex = end;
            while (newIndex - 1 >= 0 && arr[newIndex] < arr[newIndex - 1]) {
                swap(arr, newIndex, newIndex - 1);
                newIndex--;
            }
        }
        return arr;
    }

    /**
     * 插入排序（优化版）
     *
     * @param arr
     * @return
     */
    public static int[] insertSort2(int[] arr) {
        int n = arr.length;
        for (int end = 1; end < n; end++) {
            for (int pre = end - 1; pre >= 0 && arr[pre] > arr[pre + 1]; pre--) {
                swap(arr, pre, pre + 1);
            }
        }
        return arr;
    }

}
