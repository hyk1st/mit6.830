package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 *
 * 将从子运算符中读出的图元插入到构造函数中指定的tableId中。
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private static final TupleDesc resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});
    private TransactionId t;
    private OpIterator child;
    private int tableId, count;
    private boolean status;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.child = child;
        this.t = t;
        this.tableId = tableId;
        this.count = 0;
        this.status = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return Insert.resultTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();

    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * 在构造函数指定的tableId中插入从child读取的tuples。
     * 它返回一个包含插入记录数量的单字段元组。
     * 插入应该通过BufferPool来进行。
     * BufferPool的实例可以通过Database.getBufferPool()获得。
     * 注意，插入不需要检查某个元组是否是重复的。
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     *         一个包含插入记录数量的单字段元组，或者如果调用多次则为空。如果调用超过一次则为空。
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.status) return null;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, this.child.next());
                this.count++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Tuple tuple = new Tuple(Insert.resultTd);
        tuple.setField(0, new IntField(this.count));
        this.status = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
