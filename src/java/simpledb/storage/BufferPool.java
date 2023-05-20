package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * 缓冲池（BufferPool）管理从磁盘向内存读写页面的工作。磁盘上的页面的读写。访问方法调用它来检索页面，它从适当的位置取回页面。
 *
 * BufferPool也负责加锁；当一个事务获取一个页面时，BufferPool检查该事务是否有适当的锁来读/写该页面。
 *
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private int maxPage;
    private static ConcurrentHashMap<PageId, Page> map;
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    public LRU<PageId> lruManage;

    public LockManage lockManage = new LockManage();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxPage = numPages;
        map = new ConcurrentHashMap<>();
        this.lruManage = new LRU<PageId>();
    }
    public int getMaxPageNum() {
        return this.maxPage;
    }
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int type = perm == Permissions.READ_ONLY? 0 : 1;
        final long now = System.currentTimeMillis();
        while (!this.lockManage.acquireLock(pid,tid,type)) {
            if (System.currentTimeMillis() - now > 1000) {
                this.transactionComplete(tid, false);

                throw  new TransactionAbortedException();
            }
        }
        Page temp = map.get(pid);
        if (temp == null) {
            synchronized (lruManage) {
                if (map.size() >= this.maxPage) {
                    try {
                        this.evictPage();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                temp = dbFile.readPage(pid);
                if (temp == null) {
                    throw new DbException("no such page");
                }
//            System.out.println(pid.getPageNumber());
                map.put(pid, temp);
            }
        }
        lruManage.put(pid);
        return temp;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManage.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManage.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        HashSet<PageId> hset = this.lockManage.getPageIdByTid(tid);
        if (commit) {
            try {
                if (hset == null) {
                    return;
                }
                for (PageId pid : hset) {
                    Page page = map.get(pid);
                    if (page == null || page.isDirty() == null) continue;
                    if (tid.equals(page.isDirty())) {
                        flushPage(pid);
                        page.setBeforeImage();
                    }
                }
//                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (hset == null) {
                return;
            }
            for (PageId pid : hset) {
                Page page = map.get(pid);
                if (page == null || page.isDirty() == null) continue;
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page page1 = dbFile.readPage(pid);
                if (page1 == null) {
                    System.out.println("read page error on BufferPool 184");
                }
                map.put(pid, page1);
            }
        }
        if (hset == null) {
            return;
        }
        for (PageId pid : hset) {
            this.lockManage.releaseLock(pid, tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     *
     * 代表交易tid向指定的表添加一个元组。
     * 将在元组被添加到的页面和任何其他被更新的页面上获取一个写锁（lab2不需要获取锁）。
     * 如果不能获得锁，可能会阻塞。
     *
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * 通过调用markDirty位，将任何被操作弄脏的页面标记为脏页，
     * 并将任何被弄脏的页面的版本添加到缓存中（替换这些页面的任何现有版本），
     * 这样 以便未来的请求看到最新的页面。
     *
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> list = dbFile.insertTuple(tid, t);
        for (Page page : list) {
            page.markDirty(true, tid);
            map.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * 从缓冲池中删除指定的元组。
     * 将获得一个写锁，锁在被移除元组的页面和任何被更新的
     * 其他被更新的页面的写锁。如果不能获得锁，可能会阻塞。
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * 通过调用markDirty位，将任何被操作弄脏的页面标记为脏页，
     * 并将任何被弄脏的页面的版本添加到缓存中（替换这些页面的任何现有版本），
     * 这样以便未来的请求看到最新的页面。
     *
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pageId.getTableId());
        List<Page> list = dbFile.deleteTuple(tid, t);
        for (Page page : list) {
            page.markDirty(true, tid);
            map.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Collection<Page> collection = map.values();
        for (Page page : collection) {
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

     从缓冲池中删除特定的页面ID。恢复管理器需要这样做，以确保缓冲池不在其缓存中保留回滚的页面。

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        BufferPool.map.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = map.get(pid);
        TransactionId dirtier = p.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(map.get(pid));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Map.Entry<PageId, Page>> it =  map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<PageId, Page> entry = it.next();
            if (tid.equals(entry.getValue().isDirty())) {
                flushPage(entry.getKey());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
        this.lruManage.initIter();
        while (this.lruManage.hasNext()) {
            PageId pid = this.lruManage.next();
            if (map.get(pid).isDirty() == null) {
//                System.out.println("find page");
                this.discardPage(pid);
                this.lruManage.remove(pid);
                return;
            }
        }
//        System.out.println("find error");
        throw new DbException("all page is dirty");
//        if (map.get(pageId).isDirty() != null)
//            this.flushPage(pageId);
//        this.discardPage(pageId);
    }

}
