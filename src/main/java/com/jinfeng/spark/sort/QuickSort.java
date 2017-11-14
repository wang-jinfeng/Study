package com.jinfeng.spark.sort;

/**
 * Project : Spark
 * Package : com.jinfeng.spark.sort
 * Author : WangJinfeng
 * Date : 2017-10-30 21:34
 * Email : jinfeng.wang@yoyi.com.cn
 * Phone : 152-1062-7698
 *
 * 快速排序是不稳定的排序
 * 快速排序的时间复杂度
 *      平均和最好：O(nlog2n)
 *      最坏：O(n^2)
 * 当n较大时使用快速排序比较好，当序列基本有序时用快排反而不好
 */
public class QuickSort {
    /**
     * 快速排序
     * 查找出中轴（默认是最低位low）的在numbers数组排序后所在位置
     *
     * @param numbers 带查找数组
     * @param low     开始位置
     * @param high    结束位置
     * @return 中轴所在位置
     */
    public static int getMiddle(int[] numbers, int low, int high) {
        int temp = numbers[low];//  数组的第一个作为中轴
        while (low < high) {
            while (low < high && numbers[high] > temp) {
                high--;
            }
            numbers[low] = numbers[high];//    比中轴小的记录移到低端
            while (low < high && numbers[low] < temp) {
                low++;
            }
            numbers[high] = numbers[low];//   比中轴大的记录移到高端
        }
        numbers[low] = temp;//    中轴记录到尾
        return low;//   返回中轴的位置
    }

    /**
     * 递归形式的分治排序算法
     *
     * @param numbers 待排序数组
     * @param low     开始位置
     * @param high    结束位置
     */
    public static void quickSort(int[] numbers, int low, int high) {
        if (low < high) {
            int middle = getMiddle(numbers, low, high);//   将numbers数组进行一分为二
            quickSort(numbers, low, middle - 1);//  对低字段表进行递归排序
            quickSort(numbers, middle + 1, high);// 对高字段表进行递归排序
        }
    }

    /**
     * 快速排序
     *
     * @param numbers 待排序数组
     */
    public static void quick(int[] numbers) {
        if (numbers.length > 0) {  //  查看数组是否为空
            quickSort(numbers, 0, numbers.length - 1);
        }
    }
}
