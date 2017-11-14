package com.jinfeng.spark.sort;

/**
 * Project : Spark
 * Package : com.jinfeng.spark.sort
 * Author : WangJinfeng
 * Date : 2017-11-05 14:29
 * Email : wangjinfeng@yiche.com
 * Phone : 152-1062-7698
 * <p>
 * 归并排序
 * 基本思想：
 * 归并（Merge）排序法是将两个（或两个以上）有序表合并成一个新的有序表，即把待排序序列分为若干个子序列，每个子序列是有序的。
 * 然后再把有序子序列合并为整体有序序列。
 * 合并方法：
 * 设r[i...n]由两个有序子表r[i...m]和r[m+1...n]组成，两个子表长度分别为n-i+1，n-m。
 * 1.j = m + 1;k = i;i = i;    //置两个子表的起始下标及辅助数组的起始下标
 * 2.若i > m或j > n,转4；  //  其中一个子表已合并完，比较选取结束
 * 3.选取r[i]和r[j]较小的存入辅助数组rf
 * 如果r[i] < r[j],rf[k] = r[i];i++;k++;转2
 * 否则，rf[k] = r[j];j++;k++;转2
 * 4.将尚未处理完的子表中元素存入rf
 * 如果i <= m，将r[i...m]存入rf[k...n]   //前一个子表非空
 * 如果j <= n，将r[j...n]存入rf[k...n]   //后一个子表非空
 * 5.结束
 * <p>
 * 时间复杂度均为O(nlog2n)
 * 稳定排序方式
 */
public class MergeSort {

    public static void mergeSort(int[] numbers) {
        sort(numbers, 0, numbers.length - 1);
    }

    /**
     * @param numbers 待排序数组
     * @param low
     * @param high
     * @return 输出有序数组
     */
    public static int[] sort(int[] numbers, int low, int high) {

        int mid = (low + high) / 2;
        if (low < high) {
            //  左边
            sort(numbers, low, mid);
            //  右边
            sort(numbers, mid + 1, high);
            //  左右合并
            merge(numbers, low, mid, high);
        }
        return numbers;
    }

    /**
     * 将数组中low到high位置的数进行排序
     *
     * @param numbers 待排序数组
     * @param low     待排的开始位置
     * @param mid     待排的中间位置
     * @param high    待排的结束位置
     */
    public static void merge(int[] numbers, int low, int mid, int high) {
        int[] temp = new int[high - low + 1];
        int i = low;//  左指针
        int j = mid + 1;//    右指针
        int k = 0;
        //  把较小的数先移到新数组中
        while (i <= mid && j <= high) {
            if (numbers[i] < numbers[j]) {
                temp[k++] = numbers[i++];
            } else {
                temp[k++] = numbers[j++];
            }
        }

        //  把左边剩余的数移到数组
        while (i <= mid) {
            temp[k++] = numbers[i++];
        }

        //  把右边剩余的数移到数组
        while (j <= high) {
            temp[k++] = numbers[j++];
        }

        //  把新数组中的数覆盖numbers数组
        for (int k2 = 0; k2 < temp.length; k2++) {
            numbers[k2 + low] = temp[k2];
        }
    }
}
