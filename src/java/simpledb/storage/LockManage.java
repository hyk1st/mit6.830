package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LockManage {

    public class Lock{
        private TransactionId tid;
        private int type;

        public TransactionId getTid() {
            return tid;
        }

        public void setTid(TransactionId tid) {
            this.tid = tid;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public Lock(TransactionId tid, int type) {
            this.tid = tid;
            this.type = type;
        }
    }

    public class LockList {
        private List<Lock> list;
        LockList() {
            this.list = new ArrayList<>();
        }
        public synchronized boolean acquireLock(final TransactionId tid, final int lockType) {
            if (lockType == 0) {
                if (list.isEmpty()) {
                    list.add(new Lock(tid, lockType));
                    return true;
                }
                if (list.get(0).getType() == 0) {
                    for (Lock lock : list) {
                        if (lock.tid.equals(tid)) {
                            return true;
                        }
                    }
                    list.add(new Lock(tid, lockType));
                    return true;
                }
                if (list.get(0).getTid().equals(tid))
                    return true;
                return false;

            }
            if (list.isEmpty()) {
                list.add(new Lock(tid, lockType));
                return true;
            }
            if (list.size() == 1 && list.get(0).getTid().equals(tid)) {
                if (list.get(0).getType() < lockType) {
                    list.get(0).setType(lockType);
                }
                return true;
            }
            return false;
        }

        public synchronized int releaseLock(TransactionId tid) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getTid().equals(tid)) {
                    list.remove(i);
                    return list.size() == 0? 2 : 1;
                }
            }
            return 0;
        }

        public synchronized boolean holdsLock(TransactionId tid) {
            for (Lock lock : list) {
                if (lock.getTid().equals(tid)) {
                    return true;
                }
            }
            return false;
        }

    }
    ConcurrentHashMap<PageId, LockList> map;
    HashMap<TransactionId, HashSet> tMap;
    public LockManage() {
        this.map = new ConcurrentHashMap<>();
        this.tMap = new HashMap<>();
    }
    public HashSet<PageId> getPageIdByTid(TransactionId tid){
        return this.tMap.get(tid);
    }
    public synchronized boolean acquireLock(final PageId pageId, final TransactionId tid, final int lockType) {
        if (!this.map.containsKey(pageId)) {
            this.map.put(pageId, new LockList());
        }
        if (!this.tMap.containsKey(tid)) {
            this.tMap.put(tid, new HashSet());
        }
        if (this.map.get(pageId).acquireLock(tid, lockType)) {
            this.tMap.get(tid).add(pageId);
            return true;
        }
        return false;
    }

    // 释放锁
    public synchronized boolean releaseLock(final PageId pageId, final TransactionId tid) {
        if (this.tMap.containsKey(tid)) {
            this.tMap.remove(tid);
        }
        if (!this.map.containsKey(pageId)) {
            return false;
        }
        int ans = this.map.get(pageId).releaseLock(tid);
        if (ans == 2) {
            this.map.remove(pageId);
        }
        if (ans == 1) return true;
        return false;
    }

    //判断是否持有锁
    public boolean holdsLock(final PageId pageId, final TransactionId tid) {
        if (!this.map.containsKey(pageId)) {
            return false;
        }
        return this.map.get(pageId).holdsLock(tid);
    }

}
