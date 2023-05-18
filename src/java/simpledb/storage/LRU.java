package simpledb.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LRU<K> implements Iterator {

    @Override
    public boolean hasNext() {
        return this.iter.pre != this.head;
    }

    @Override
    public K next() {
        this.iter = this.iter.pre;
        return this.iter.key;
    }

    // LruCache node
    public class Node {
        public Node pre;
        public Node next;
        public K    key;

        public Node(final K key) {
            this.key = key;
        }
    }

    private final Map<K, Node> nodeMap;
    private final Node         head;
    private final Node         tail;
    private Node iter;
    public LRU() {
        this.head = new Node(null);
        this.tail = new Node(null);
        this.head.next = tail;
        this.tail.pre = head;
        this.nodeMap = new HashMap<>();
    }
    public int getSize() {
        return this.nodeMap.size();
    }
    public void linkToHead(Node node) {
        Node next = this.head.next;
        node.next = next;
        node.pre = this.head;

        this.head.next = node;
        next.pre = node;
    }

    public void moveToHead(Node node) {
        removeNode(node);
        linkToHead(node);
    }

    public void removeNode(Node node) {
        if (node.pre != null && node.next != null) {
            node.pre.next = node.next;
            node.next.pre = node.pre;
        }
    }

    public K removeLast() {
        Node last = this.tail.pre;
        removeNode(last);
        return last.key;
    }
    public void initIter(){
        this.iter = this.tail;
    }

    public synchronized void remove(K key) {
        if (this.nodeMap.containsKey(key)) {
            final Node node = this.nodeMap.get(key);
            removeNode(node);
            this.nodeMap.remove(key);
        }
    }

    public synchronized void get(K key) {
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            moveToHead(node);
        }
    }

    public synchronized void put(K key) {
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            moveToHead(node);
        } else {
            Node node = new Node(key);
            this.nodeMap.put(key, node);
            linkToHead(node);
        }
    }
}
