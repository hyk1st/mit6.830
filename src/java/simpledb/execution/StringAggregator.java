package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldType;
    private Op what;
    private HashMap<Field, Integer> map;

    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        map = new HashMap<>();
        if(gbfield != Aggregator.NO_GROUPING){
            this.td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE,});
        }else{
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        if (what != Op.COUNT) {
            throw  new IllegalArgumentException("op != count");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int temp;
        if (this.gbfield != Aggregator.NO_GROUPING) {
            temp = map.computeIfAbsent(tup.getField(this.gbfield), k -> new Integer(0));
            //            map.get(tup.getField(this.gbfield)).merge(value);
        } else {
            temp = map.computeIfAbsent(null, k -> new Integer(0));
        }
        map.put(tup.getField(this.gbfield), temp + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>(map.size());
        for (Map.Entry<Field, Integer> entry : map.entrySet()) {
            Tuple tp = new Tuple(td);
            if(gbfield != Aggregator.NO_GROUPING){
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue()));
            }else{
                tp.setField(0, new IntField(entry.getValue()));
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
