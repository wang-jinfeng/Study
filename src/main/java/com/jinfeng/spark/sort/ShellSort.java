package com.jinfeng.spark.sort;

/**
 * Project : Spark
 * Package : com.jinfeng.spark.sort
 * Author : WangJinfeng
 * Date : 2017-11-05 13:40
 * Email : wangjinfeng@yiche.com
 * Phone : 152-1062-7698
 *
 * 基本思想：
 * 先将整个待排序的记录序列分割成为若干子序列分别进行直接插入排序，待整个序列中的记录
 * "基本有序"时，再对全体记录进行依次直接插入排序
 * 操作方法：
 * 1.选择一个增量序列t1,t2,...,tk,其中ti>tj,tk=1;
 * 2.按增量序列个数k，对序列进行k趟排序
 * 3.每趟排序，根据对应的增量ti,将待排序列分割成若干个长度为m的子序列，分别对个字表
 * 进行直接插入排序。仅增量因子为1时，整个序列作为一个表来处理，表长度即为整个序列的
 * 长度。
 *
 * 时间复杂度
 *      平均：O(n^1.3)
 *      最好：O(n)
 *      最坏：O(n^2)
 *
 * 不稳定排序方式
 */
public class ShellSort {
    /**
     * 希尔排序的原理：根据需求，如果你想要结果从大到小排列，它会首先将数组进行分组，然后将较大
     * 值移到前面，较小值移到后面，最后将整个数组进行插入排序，这样比起一开始就用插入排序减少了
     * 数据交换和移到的次数，可以说希尔排序是加强版的插入排序
     * <p>
     * 拿数组5，2，8，9，1，3，4来说，数组长度为7，当increment为3时，数组分为两个序列5，2，8和
     * 9，1，3，4，第一次排序，9和5比较，1和2比较，3和8比较，4和比其下标值小increment的数组相比较，
     * 此例子是按照从大到小排列，所以大的会排在前面，第一次排序后数组为9，2，8，5，1，3，4
     * 第一次后increment的值变为3/2 = 1，此时对数组进行插入排序，实现数组从大到小排列
     */

    public static void shellSort(int[] numbers) {
        int j = 0;
        int temp = 0;
        //  每次将步长缩短为原来的一半
        for (int increment = numbers.length / 2; increment > 0; increment /= 2) {
            for (int i = increment; i < numbers.length; i++) {
                temp = numbers[i];
                for (j = i; j >= increment; j -= increment) {
                    //  如想从小到大排只需修改这里
                    if (temp > numbers[j - increment]) {
                        numbers[j] = numbers[j - increment];
                    } else {
                        break;
                    }
                }
                numbers[j] = temp;
            }
        }
    }
}
