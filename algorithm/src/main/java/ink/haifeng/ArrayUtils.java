package ink.haifeng;

public class ArrayUtils {

    public static void main(String[] args) {

        int[] arr=new int[8];
        for (int i = 0; i < 20000000; i++) {
            int idx = (int) (Math.random() * 8);
            arr[idx]= arr[idx]+1;
        }
        for (int i = 0; i < arr.length; i++) {
            System.out.println(i+"\t"+(double)arr[i]/(double)20000000);
        }
        int[] array = randomArray(20, 20);
        printArr(array);
    }

    public static void printArr(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i]);
            if (i != arr.length - 1) {
                System.out.print(",");
            }
        }
        System.out.println();
    }


    /**
     * 生成一个随机数组 最长为maxSize，最大值小于maxValue
     * @param maxSize
     * @param maxValue
     * @return
     */
    public static int[] randomArray(int maxSize, int maxValue) {
        int[] arr = new int[(int) ((maxSize + 1) * Math.random())];
        for (int i = 0; i < arr.length; i++) {
            arr[i] =  (int) (maxValue * Math.random());
        }
        return arr;
    }


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
}
