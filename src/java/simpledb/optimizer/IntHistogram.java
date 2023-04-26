package simpledb.optimizer;

import simpledb.execution.Predicate;

import javax.xml.bind.annotation.XmlInlineBinaryData;
import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 * 一个用于表示基于单一整数字段的固定宽度直方图的类。
 */
public class IntHistogram {


    private int bucketNum, min, max, size, sum, lasSize;
    private int[] bucket;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     *
     * 创建一个新的IntHistogram。
     * 这个IntHistogram应该维护一个它所收到的整数值的直方图。
     * 它应该把直方图分成 "桶 "buckets。
     * 被直方图化的值将通过 "addValue() "函数一次次地提供。
     * 你的实现应该使用的空间和执行时间与被直方图化的值的数量相比都是恒定的。
     * 例如，你不应该 简单地将你看到的每个值都存储在一个排序的列表中。
     *
     *
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.bucketNum = buckets;
        this.min = min;
        this.max = max;
        this.sum = 0;
        if (this.max - this.min + 1 < this.bucketNum) {
            this.bucketNum = this.max - this.min + 1;
        }
        if ((this.max - this.min + 1) % this.bucketNum == 0)
            this.size = (this.max - this.min + 1) / this.bucketNum;
        else this.size = (this.max - this.min + 1) / (this.bucketNum - 1);
        this.bucket = new int[this.bucketNum];
        this.lasSize = this.max - this.min + 1 - (this.bucketNum - 1) * this.size;
    }


    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v >= this.min && v <= this.max) {
            v -= this.min;
            v /= this.size;
            v = Math.min(v, this.bucketNum - 1);
            this.bucket[v]++;
            this.sum++;
        }
    }

    public int getSize(int id) {
        return (id == this.bucketNum - 1 ? this.lasSize : this.size);
    }

    public double less(int v) {
        if (v >= this.max) return 1;
        if (v < this.min) return 0;
        v -= this.min;
        int id = v / this.size;
        id = Math.min(id, this.bucketNum - 1);
        double ans = 0;
        for (int i = 0; i < id; i++)
            ans += 1.0 * this.bucket[i] / this.sum;
        ans += 1.0 * (v % getSize(id)) / getSize(id) * (1.0 * this.bucket[id] / this.sum);
        return ans;
    }

    public double equal(int v) {
        if (v < min || v > max) return 0;
        v -= this.min;
        int id = v / this.size;
        id = Math.min(id, this.bucketNum - 1);
        double ans = 0;
        ans += 1.0 / getSize(id) * (1.0 * this.bucket[id] / this.sum);
        return ans;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 在这个表格上估计一个特定的谓词和操作数的选择性。
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * 例如，如果 "op "是 "GREATER_THAN"，"v "是5、
     * return your estimate of the fraction of elements that are greater than 5.
     * 返回你对大于5的元素所占比例的估计。
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        switch (op) {
            case GREATER_THAN_OR_EQ:
                return 1.0 - less(v);
            case NOT_EQUALS:
                return 1.0 - equal(v);
            case EQUALS:
                return equal(v);
            case GREATER_THAN:
                return 1.0 - less(v) - equal(v);
            case LESS_THAN:
                return less(v);
            case LESS_THAN_OR_EQ:
                return less(v) + equal(v);
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     *
     *     这个直方图的平均选择性。
     *     这并不是实现基本连接优化的一个不可或缺的方法。
     *     如果你想实现一个更有效的优化，它可能是需要的
     *
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(bucket);
    }
}
