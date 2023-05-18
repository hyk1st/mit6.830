package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * HeapFile是DbFile的一个实现，它存储了一个没有特定顺序的图元集合。
 * 图元被存储在页面上，每个页面的大小都是固定的，而文件只是这些页面的集合。
 * HeapFile与HeapPage紧密合作。HeapPages的格式在HeapPage的 构造函数中描述的。
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File file;
    private TupleDesc tupleDesc;


    /**
     * Constructs a heap file backed by the specified file.
     * 构造一个由指定文件支持的堆文件。
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     *返回磁盘上支持这个HeapFile的文件。
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public Page readPage(int pid) {
        return readPage(new HeapPageId(this.getId(), pid));
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getPageNumber() >= this.numPages()) {
            return null;
        }
//        System.out.println(pid.getPageNumber());
        long offset = (long) (pid.getPageNumber()) * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
//            System.out.println("offset " + offset+ ", page " + pid.getPageNumber());
            RandomAccessFile randomAccessFile =  new RandomAccessFile(file, "rw");
            randomAccessFile.seek(offset);
            for (int i = 0; i < BufferPool.getPageSize(); i++) {
                data[i] = (byte) randomAccessFile.read();
            }
            randomAccessFile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            System.out.println("error: \n" + e.getMessage()+  "   \nheapfile read page error on " + pid.getPageNumber());
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        long offset = (long) (pid.getPageNumber()) * BufferPool.getPageSize();
        try {
//            System.out.println("offset " + offset+ ", page " + pid.getPageNumber());
            RandomAccessFile randomAccessFile =  new RandomAccessFile(file, "rw");
            randomAccessFile.seek(offset);
            randomAccessFile.write(page.getPageData());
            randomAccessFile.close();
        } catch (Exception e) {
            System.out.println("write page  page error on " + pid.getPageNumber());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage page;
        for (int pgNo = 0; ; pgNo++) {
            HeapPageId pageId = new HeapPageId(getId(), pgNo);
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                if (page.getNumEmptySlots() == 0) {
                    continue;
                }
            } catch (DbException e) {
                page = new HeapPage(pageId, HeapPage.createEmptyPageData());
                writePage(page);
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            }
            break;
        }
        page.insertTuple(t);
        page.markDirty(true, tid);
        return Collections.singletonList(page);
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage page;
        RecordId recordId = t.getRecordId();
        if (getId() != recordId.getPageId().getTableId()) {
            throw new DbException(String.format("tableId not equals %d != %d", getId(), recordId.getPageId().getTableId()));
        }
        HeapPageId pageId = new HeapPageId(getId(), recordId.getPageId().getPageNumber());

        page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return Collections.singletonList(page);
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }


    private class HeapFileIterator implements DbFileIterator {

        private int pageNo;
        private TransactionId transactionId;
        private Iterator<Tuple> iterator;
        private boolean status;
        public HeapFileIterator(TransactionId tid) {
            this.transactionId = tid;
            this.status = false;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPage temp = (HeapPage) Database.getBufferPool().getPage(transactionId, new HeapPageId(getId(), 0), Permissions.READ_ONLY);

            if (temp == null) {
                throw new DbException("file open error on HeapFile.open()");
            }
            this.pageNo = 0;
            this.iterator = temp.iterator();
            this.status = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.status) {
                return false;
            }
            if (this.iterator.hasNext()) {
                return true;
            }
            if (this.pageNo + 1 >= numPages()) {
                return false;
            }
            HeapPage temp = (HeapPage) Database.getBufferPool().getPage(transactionId, new HeapPageId(getId(), this.pageNo + 1), Permissions.READ_ONLY);
            this.iterator = temp.iterator();
            this.pageNo++;
            return this.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.status) {
                throw new NoSuchElementException("not open");
            }
            if (this.hasNext()) {
                return this.iterator.next();
            }
            throw new NoSuchElementException("No such element in heapfile tuple");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!this.status) {
                throw new DbException("not open");
            }
            this.pageNo = 0;
            HeapPage temp = (HeapPage) Database.getBufferPool().getPage(transactionId, new HeapPageId(getId(), 0), Permissions.READ_ONLY);
            this.iterator = temp.iterator();
        }

        @Override
        public void close() {
            if (this.status == false) return;
            this.status = false;
        }
    }
}

