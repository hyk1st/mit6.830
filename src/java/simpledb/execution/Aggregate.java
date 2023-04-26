package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 *
 * 聚合运算符，计算聚合（例如，sum, avg, max、 min）。
 * 请注意，我们只支持按单列分组的单列上的聚合。
 *
 */
public class  Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield, gfield;
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * 实现提示：根据Afield的类型，
     * 你需要构造一个{@link IntegerAggregator}或{@link StringAggregator}来帮助你实现readNext()。
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        Type type = null;
        if (this.gfield != Aggregator.NO_GROUPING) {
            type = this.child.getTupleDesc().getFieldType(this.gfield);
        }
        if (this.child.getTupleDesc().getFieldType(this.afield) == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(this.gfield, type, this.afield, aop);
        } else {
            this.aggregator = new IntegerAggregator(this.gfield, type, this.afield, aop);
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (this.gfield == Aggregator.NO_GROUPING) {
            return  null;
        }
        return child.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.iterator = this.aggregator.iterator();
        this.iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     *
     * 返回下一个元组。
     * 如果有一个group by字段，那么第一个字段是我们要分组的字段，第二个字段是计算集合的结果。
     * 如果没有按字段分组，那么结果元组应该包含一个字段，代表聚合的结果。如果没有更多的元组，应该返回null。
     *
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.iterator.hasNext()) return this.iterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * 返回该聚合体的TupleDesc。
     * 如果没有group by字段，这将有一个字段--聚合列。
     * 如果有一个group by字段，第一个字段将是group by字段，第二个字段将是 聚合值列。
     *
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     *
     * 聚合列的名称应具有信息性。
     * 比如说： "aggName(aop) (child_td.getFieldName(afield))"
     * 其中aop和afield在构造函数中给出，child_td是孩子的TupleDesc 迭代器。
     *
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.aggregator.getTupleDesc();
    }

    public void close() {
        // some code goes
        super.close();
        this.iterator.close();
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
