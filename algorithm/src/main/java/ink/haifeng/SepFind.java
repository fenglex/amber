package ink.haifeng;

import cn.hutool.core.util.RandomUtil;

public class SepFind {
    public static void main(String[] args) {
//        for (int i = 0; i < 500; i++) {
//            int[] arr = ArrayUtils.randomArray(20, 100);
//            int[] sortArr = ArrayUtils.bubbleSort(arr);
//            int random = (int) (100 * Math.random());
//            if (mostLeftNoLessNumIndex(sortArr, random) != mostLeftNoLessNumIndexCheck(sortArr, random)) {
//                System.out.println("出错了");
//            }
//        }
//        int[] arr = ArrayUtils.randomArray(20, 100);


        int[] arr={1,2,3,4,5,6,7};
        System.out.println(findMinIndex(arr));
        int length=100;
        int maxValue=1000;
        System.out.println("开始了");
        for (int i = 0; i <200000 ; i++) {
            int[] indexArr = minIndexArr(length, maxValue);
            int minIndex = findMinIndex(indexArr);
            boolean flag = checkMinIndex(indexArr, minIndex);
            if (!flag){
                System.out.println("出错了");
            }
        }
        System.out.println("结束了");
    }


    /**
     * arr 有序，找到 >=num 的最左的位置
     *
     * @param arr
     * @param num
     * @return
     */
    public static int mostLeftNoLessNumIndex(int[] arr, int num) {
        if (arr == null || arr.length == 0) {
            return -1;
        }
        int l = 0, r = arr.length - 1, ans = -1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (arr[mid] >= num) {
                ans = mid;
                r = mid - 1;
            } else {
                l = mid + 1;
            }

        }
        return ans;
    }


    public static int mostLeftNoLessNumIndexCheck(int[] arr, int num) {
        int ans = -1;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] >= num) {
                ans = i;
                break;
            }
        }
        return ans;
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


    /**
     * 找到一个局部最小值（随意一个）
     * 局部最小:  n位置的值小于n-1且n位置的值小于n+1位置的值
     * @param arr arr是一个相邻位置不等的数组
     * @return
     */
    public static int findMinIndex(int[] arr) {
        if (arr == null || arr.length == 0) {
            return -1;
        }
        int n = arr.length;
        if (n == 1) {
            return 0;
        }
        if (arr[0] < arr[1]) {
            return 0;
        }
        if (arr[n - 1] < arr[n - 2]) {
            return n - 1;
        }
         // 上面的条件已经返回两端最小的情况，限定了开始是下降，结束是上升的,中间一定存在一个局部最小值（因为相邻两个数不相等）
        int l = 0, r = arr.length - 1;
        while (l < r - 1) {
            int mid = (l + r) / 2;
            if (arr[mid] < arr[mid - 1] && arr[mid] < arr[mid + 1]) {
                // mid-1>mid, mid<mid+1
                return mid;
            } else {
                // 只存在一下三种情况
                // 1、 mid-1>mid,mid>mid+1 右侧查找
                // 2、 mid-1<mid,mid<mid+1 左侧查找
                // 3、 mid-1<mid,mid>mid+1 左侧查找
                 // 因为一定存在局部最小值
                // 如果此时l和r相邻了，跳出循环对于以上三种情况
                //  mid>mid-1
                //  左侧查找  l=mid-2, r=mid-1,如果mid-2<mid-1 ,mid-1<mid,继续进入循环因为一定有最小局部
                //  如果mid-2>mid-1 ,mid-1<mid会进入126行
                //
                //  mid<mid-1 右侧查找  l=mid+1,r=mid+2 ,  此时mid>mid+1,mid+1<mid+2,此时返回mid+1,符合 arr[l] < arr[r] ? l : r;
                if (arr[mid] > arr[mid - 1]) {
                    r = mid - 1;
                } else {
                    l = mid + 1;
                }
            }
        }
        return arr[l] < arr[r] ? l : r;
    }

    /**
     * 生成一个相邻位置不相等的数组
     *
     * @param maxLength
     * @param maxValue
     * @return
     */
    public static int[] minIndexArr(int maxLength, int maxValue) {
        int length = (int) (Math.random() * maxLength)+1;
        int[] arr = new int[length];
        arr[0] = (int) (Math.random() * maxValue);
        for (int i = 1; i < length; i++) {
            do {
                arr[i] = (int) (Math.random() * maxValue);
            } while (arr[i - 1] == arr[i]);
        }
        return arr;
    }

    public static boolean checkMinIndex(int[] arr, int index) {
        if (arr == null || arr.length == 0) {
            return index == -1;
        }
        int left = index - 1;
        int right = index + 1;
        boolean leftBigger = left >= 0 ? arr[left] > arr[index] : true;
        boolean rightBigger = right <= arr.length - 1 ? arr[right] > arr[index] : true;
        return leftBigger && rightBigger;

    }

}
