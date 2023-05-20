package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 *
 * TableStats表示关于基表的统计数据（例如，直方图）在一个 查询中的基表的统计数据（如直方图）。
 *
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     *
     * 直方图的分仓数。尽管我们的测试假设你的直方图至少有100个bin，但你可以随意增加这个值。直方图。
     *
     */
    static final int NUM_HIST_BINS = 100;
    private int pageNum, tupleNum, ioCost;
    private HashMap<Integer, IntHistogram> map;
    private String tableName;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * 创建一个新的TableStats对象，它可以跟踪表的每个 列的统计数据
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage)  {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCost = ioCostPerPage;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.tableName = Database.getCatalog().getTableName(tableid);
        TupleDesc tupleDesc = dbFile.getTupleDesc();
        this.pageNum = ((HeapFile)dbFile).numPages();
        this.map = new HashMap<>();
        this.tupleNum = 0;
        HashMap<Integer, ArrayList<Integer>> hmap = new HashMap<>();
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            hmap.put(i, new ArrayList<>());
        }
        SeqScan iterator = new SeqScan(new TransactionId(), dbFile.getId());
//        DbFileIterator iterator = dbFile.iterator(null);
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple tupleTemp = iterator.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) != Type.INT_TYPE) continue;
                    hmap.get(i).add(((IntField)tupleTemp.getField(i)).getValue());
                }
                this.tupleNum++;
            }
            iterator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            if (tupleDesc.getFieldType(i) != Type.INT_TYPE) continue;
            int min = Collections.min(hmap.get(i)), max =  Collections.max(hmap.get(i));
            IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, min, max);
            for (Integer integer : hmap.get(i)) {
                intHistogram.addValue(integer);
            }
            this.map.put(i, intHistogram);
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * 估计顺序扫描文件的成本，考虑到读取一个页面的成本是costPerPageIO。
     * 你可以假设没有寻道，也没有页面在缓冲池中。
     * 另外，假设你的硬盘只能一次读取整个页面，所以如果表的最后一页只有一个元组，它的读取成本和整个页面一样高。
     * (大多数真正的硬盘不能 大多数真正的硬盘不能有效地一次处理小于一页的区域）。
     *
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.pageNum * this.ioCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * 该方法返回关系中图元的数量，鉴于应用了具有选择性的selectivityFactor的谓词。
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     *
     * 该方法的语义是：给定表，然后给定一个元组，我们不知道其中字段的值，返回预期的选择性。你可以从直方图中估计这个值。
     *
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return this.map.get(field).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * 估计表上的谓语字段op常数的选择性。
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        return this.map.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here

        return this.tupleNum;
    }

}
