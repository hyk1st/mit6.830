package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 * 连接操作符实现了关系连接操作。
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator child1, child2;
    private Tuple temp1, temp2;
    private ArrayList<Tuple> arrayList;
    private int id, cacheSize;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        temp1 = temp2 = null;
        this.arrayList = new ArrayList<>();
        this.cacheSize = (int) Math.floor((BufferPool.getPageSize() * 8.0) / (this.child1.getTupleDesc().getSize() * 8.0 + 1.0));
        this.cacheSize *= Database.getBufferPool().getMaxPageNum() - 2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child1.open();
        this.child2.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child1.rewind();
        this.child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * 返回由连接产生的下一个元组，如果没有更多的元组，则返回空。
     * 从逻辑上讲，这是在r1交叉r2中满足连接谓词的下一个元组。有许多可能的实现方式； 最简单的是一个嵌套的循环连接。
     *
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * 请注意，从这个特定的Join实现中返回的图元仅仅是来自左右关系的连接图元的串联。
     * 因此，如果使用一个平等的谓词，在结果中会有两份连接属性的副本。
     * (删除这种重复的列，如果需要的话，可以用一个额外的投影操作符来完成）。
     *
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * <p>
     * 例如，如果一个元组是{1,2,3}，另一个元组是{1,5,6}、 在第一列相等的情况下连接，那么这将返回{1,2,3,1,5,6}。
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.child1.hasNext() || arrayList.size() > 0) {
            if (arrayList.size() == 0) {
                for (int i = 0; i < this.cacheSize && this.child1.hasNext(); i++) {
                    this.arrayList.add(this.child1.next());
                }
            }
            while (this.child2.hasNext() || this.temp2 != null) {
                if (this.temp2 == null) {
                    this.temp2 = this.child2.next();
                    this.id = 0;
                }
                for (; id < this.arrayList.size(); id++) {
                    this.temp1 = this.arrayList.get(id);
                    if (this.p.filter(this.temp1, this.temp2)) {
                        TupleDesc tupleDesc = this.getTupleDesc();
                        int i = 0;
                        Tuple ans = new Tuple(tupleDesc);
                        for (Iterator<Field> it = this.temp1.fields(); it.hasNext(); ) {
                            ans.setField(i++, it.next());
                        }
                        for (Iterator<Field> it = this.temp2.fields(); it.hasNext(); ) {
                            ans.setField(i++, it.next());
                        }
                        id++;
                        return ans;
                    }
                }
                temp2 = null;
            }
            this.arrayList.clear();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }


}