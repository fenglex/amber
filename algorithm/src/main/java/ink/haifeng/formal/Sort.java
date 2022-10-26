package ink.haifeng.formal;

/**
 * 一些常见的排序
 *
 * @author haifeng
 */
public class Sort {
    public static void main(String[] args) {
        int[] arr = randomIntArr(10, 10);
        printArr(arr);
        bubbleSort(arr);
        printArr(arr);
        System.out.println("-------------");
        arr = randomIntArr(10, 10);
        printArr(arr);
        selectSort(arr);
        printArr(arr);
        System.out.println("-------------");
        arr = randomIntArr(10, 10);
        printArr(arr);
        insertSelect(arr);
        printArr(arr);
    }


    private static void printArr(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + ",");
        }
        System.out.println();
    }

    /**
     * 生成一个随机数组
     *
     * @param maxValue 数组最大值（不包含改值）
     * @param length   数组长度
     * @return
     */
    public static int[] randomIntArr(int maxValue, int length) {
        int[] arr = new int[length];
        for (int i = 0; i < length; i++) {
            arr[i] = (int) (maxValue * Math.random());
        }
        return arr;
    }


    /**
     * 冒泡排序
     *
     * @param arr
     */
    public static void bubbleSort(int[] arr) {
        for (int end = arr.length - 1; end > 0; end--) {
            for (int i = 0; i < end; i++) {
                if (arr[i] > arr[i + 1]) {
                    int tmp = arr[i];
                    arr[i] = arr[i + 1];
                    arr[i + 1] = tmp;
                }
            }
        }
    }


    /**
     * 选择排序
     *
     * @param arr
     */
    public static void selectSort(int[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
            int minIndex = i;
            for (int j = i; j < arr.length - 1; j++) {
                if (arr[j + 1] < arr[minIndex]) {
                    minIndex = j + 1;
                }
            }
            int tmp = arr[i];
            arr[i] = arr[minIndex];
            arr[minIndex] = tmp;
        }
    }

    public static void insertSelect(int[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = i + 1; j > 0; j--) {
                if (arr[j] < arr[j-1]) {
                    int tmp = arr[j-1];
                    arr[j-1] = arr[j];
                    arr[j] = tmp;
                } else {
                    break;
                }
            }
        }
    }
}
