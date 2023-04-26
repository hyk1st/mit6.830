package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 *
 * 知道如何在一组IntFields上计算一些聚合。
 *
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldType;
    private Op what;
    private HashMap<Field, Result> map;

    private TupleDesc td;

    private class Result {
        private int count;
        private int ans;
        Result() {
            count = ans = 0;
        }
        public void merge(int value) {
            switch (what) {
                case MIN:
                {
                    if (count == 0) ans = value;
                    else ans = Math.min(ans, value);
                }
                break;
                case MAX:
                {
                    if (count == 0) ans = value;
                    else ans = Math.max(ans, value);
                }
                break;
                case AVG:
                    ans += value;
                    break;
                case SUM:
                    ans += value;
                    break;
                case COUNT:
                    break;
                case SC_AVG:
                    ans += value;
                    break;
                case SUM_COUNT:
                    ans += value;
                    break;
            }
            count++;
        }
        public int get() {
            switch (what) {
                case MIN:
                return ans;
                case MAX:
                return ans;
                case AVG:
                return ans / count;
                case SUM:
                return ans;
                case COUNT:
                return count;
                case SC_AVG:
                return ans / count;
                case SUM_COUNT:
                return ans;
            }
            return ans;
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple 元组中聚合字段的基于0的索引
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.what = what;
        map = new HashMap<>();
        if(gbfield != Aggregator.NO_GROUPING){
            this.td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE,});
        }else{
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int value = ((IntField)tup.getField(this.afield)).getValue();
        if (this.gbfield != Aggregator.NO_GROUPING) {
            map.computeIfAbsent(tup.getField(this.gbfield), k -> new Result()).merge(value);
//            map.get(tup.getField(this.gbfield)).merge(value);
        } else {
            map.computeIfAbsent(null, k -> new Result()).merge(value);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>(map.size());
        for (Map.Entry<Field, Result> entry : map.entrySet()) {
            Tuple tp = new Tuple(td);
            if(gbfield != Aggregator.NO_GROUPING){
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue().get()));
            }else{
                tp.setField(0, new IntField(entry.getValue().get()));
            }
            tuples.add(tp);
        }
        return new TupleIterator(td, tuples);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

}
