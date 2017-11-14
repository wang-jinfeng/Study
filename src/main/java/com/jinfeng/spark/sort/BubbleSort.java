package com.jinfeng.spark.sort;

/**
 * Project : Spark
 * Package : com.jinfeng.spark.sort
 * Author : WangJinfeng
 * Date : 2017-10-30 21:33
 * Email : jinfeng.wang@yoyi.com.cn
 * Phone : 152-1062-7698
 *
 * 冒泡排序是一种稳定的排序方法
 * 1.若文件初状时为正序，则一趟起泡就可完成排序，排序码的比较次数为n-1,且没有移动记录，时间复杂度为O(n)
 * 2.若文件初态为逆序，则需要n-1趟起泡，每趟进行n-1次排序码的比较，且每次比较都移动三次，比较和移动次数均
 * 达到最大值O(n^2)
 * 3.起泡排序平均时间复杂度为O(n^2)
 *
 * 时间复杂度：
 *      平均：O(n^2)
 *      最好：O(n)
 *      最坏：O(n^2)
 * 稳定排序方式
 */
public class BubbleSort {
    /**
     * 冒泡排序
     * 比较相邻的元素。如果第一个比第二个大，就交换他们两个。
     * 对每一对相邻元素做同样的工作，从开始第一对到结尾的最后一对。在这一点，最后的元素应该会是最大的。
     * 针对所有的元素重复以上的操作，除了最后一个。
     * 持续每次对越来越少的元素重复上面的步骤，直到没有任何一对数字做比较
     *
     * @param numbers 需要排序的整型数组
     */
    public static void bubbleSort(int[] numbers) {
        int temp = 0;
        int size = numbers.length;
        for (int i = 0; i < size - 1; i++) {
            for (int j = 0; j < size - 1 - i; j++) {
                if (numbers[j] > numbers[j + 1]) {
                    temp = numbers[j];
                    numbers[j] = numbers[j + 1];
                    numbers[j + 1] = temp;
                }
            }
        }
    }
}
