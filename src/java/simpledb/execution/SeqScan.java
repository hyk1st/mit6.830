package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 *
 * SeqScan是一个顺序扫描访问方法的实现，它不按特定的顺序（例如，按它们在磁盘上的排列）读取表的每个元组。
 *
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private int tableId;
    private String aliasName;
    private DbFileIterator iterator;
    private DbFile dbFile;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * 在指定的表上创建一个顺序扫描，作为指定事务的一部分。指定事务的一部分。
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     *
     *            这个表的别名（解析器需要）；返回的tupleDesc应该有字段名tableAlias.fieldName
     *            （注意：这个类不负责处理tableAlias或fieldName为空的情况。
     *            如果它们是空的，它不应该崩溃，但是结果的名字可以是null.fieldName,tableAlias.null,或者null.null)。
     *
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.transactionId = tid;
        this.tableId = tableid;
        this.aliasName = tableAlias;
        this.dbFile = Database.getCatalog().getDatabaseFile(this.tableId);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.aliasName;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.aliasName = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.iterator = this.dbFile.iterator(this.transactionId);
        this.iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * 返回带有来自底层 HeapFile 的字段名的 TupleDesc，前缀为来自构造函数的 tableAlias 字符串。
     * 这个前缀在连接含有相同字段的表时非常有用。
     * 名称的表时，这个前缀非常有用。别名和名称应该用一个". "字符分开。
     *
     * (例如，"alias. fieldName"）。
     *
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = this.dbFile.getTupleDesc();
        Type[] types = new Type[tupleDesc.numFields()];
        String[] names = new String[tupleDesc.numFields()];
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            types[i] = tupleDesc.getFieldType(i);
            names[i] = this.aliasName + "." + tupleDesc.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return this.iterator.next();
    }

    public void close() {
        // some code goes here
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.iterator.rewind();
    }
}
