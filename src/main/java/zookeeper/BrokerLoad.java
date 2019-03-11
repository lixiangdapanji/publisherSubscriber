package zookeeper;

import java.util.*;

public class BrokerLoad {

    private class Bucket {
        int value;
        Set<String> set;
        Bucket prev;
        Bucket next;

        Bucket(int value) {
            this.value = value;
            set = new HashSet<>();
            prev = null;
            next = null;
        }
    }

    private Bucket head;
    private Bucket tail;
    private Map<Integer, Bucket> bucketsMap;
    private Map<String, Integer> keysMap;

    /**
     * Initialize your data structure here.
     */
    public BrokerLoad() {
        head = new Bucket(0);
        tail = new Bucket(0);
        head.next = tail;
        tail.prev = head;
        bucketsMap = new HashMap<>();
        keysMap = new HashMap<>();
    }

    public void removeKey(String key) {
        if (key == null || !keysMap.containsKey(key)) return;
        int value = keysMap.get(key);
        Bucket pos = bucketsMap.get(value);
        pos.set.remove(key);
        if (pos.set.isEmpty()) {
            removeBucket(pos);
        }
        keysMap.remove(key);
    }

    //choose a key with minimum load
    public String chooseMin(String[] keys) {
        if (keys.length == 0)
            return null;

        String res = null;
        int minValue = Integer.MAX_VALUE;
        for (String key : keys) {
            if (keysMap.containsKey(key) && keysMap.get(key) < minValue) {
                res = key;
                minValue = keysMap.get(key);
            }
        }
        return res;
    }

    //choose k keys with minimum value
    public List<String> chooseKey(int k) {
        List<String> ans = new ArrayList<>();

        Bucket bucket = head.next;
        while (k > bucket.set.size()) {
            ans.addAll(bucket.set);
            k -= bucket.set.size();
            bucket = bucket.next;
        }

        Iterator<String> it = bucket.set.iterator();
        while (k > 0) {
            ans.add(it.next());
            k--;
        }
        return ans;
    }

    /**
     * Inserts a new key <Key> with value 1. Or increments an existing key by 1.
     */
    public void inc(String key) {
        if (keysMap.containsKey(key)) {
            int value = keysMap.get(key);
            Bucket prevB = bucketsMap.get(value);
            int currValue = value + 1;
            keysMap.put(key, currValue);
            if (bucketsMap.containsKey(currValue)) {
                bucketsMap.get(currValue).set.add(key);
            } else {
                Bucket b = new Bucket(currValue);
                b.set.add(key);
                b.prev = prevB;
                b.next = prevB.next;
                prevB.next.prev = b;
                prevB.next = b;
                bucketsMap.put(currValue, b);
            }
            prevB.set.remove(key);
            if (prevB.set.size() == 0) {
                removeBucket(prevB);
            }
        } else {
            keysMap.put(key, 1);
            if (bucketsMap.containsKey(1)) {
                bucketsMap.get(1).set.add(key);
            } else {
                Bucket b = new Bucket(1);
                b.set.add(key);
                b.prev = head;
                b.next = head.next;
                head.next.prev = b;
                head.next = b;
                bucketsMap.put(1, b);
            }
        }
    }

    /**
     * Decrements an existing key by 1. If Key's value is 1, remove it from the data structure.
     */
    public void dec(String key) {
        if (!keysMap.containsKey(key)) {
            return;
        }
        int currValue = keysMap.get(key);
        //System.out.println(currValue);
        int updateValue = currValue - 1;
        Bucket currB = bucketsMap.get(currValue);
        currB.set.remove(key);
        if (updateValue == 0) {
            keysMap.remove(key);
        } else {
            Bucket updateB = null;
            if (bucketsMap.containsKey(updateValue)) {
                updateB = bucketsMap.get(updateValue);
            } else {
                updateB = new Bucket(updateValue);
                currB.prev.next = updateB;
                updateB.prev = currB.prev;
                updateB.next = currB;
                currB.prev = updateB;
                bucketsMap.put(updateValue, updateB);
            }
            updateB.set.add(key);
            keysMap.put(key, updateValue);
        }
        if (currB.set.size() == 0) {
            removeBucket(currB);
        }

    }

    /**
     * Returns one of the keys with maximal value.
     */
    public String getMaxKey() {
        return head.next == tail ? "" : tail.prev.set.iterator().next();
    }

    /**
     * Returns one of the keys with Minimal value.
     */
    public String getMinKey() {
        return head.next == tail ? "" : head.next.set.iterator().next();
    }

    private void removeBucket(Bucket b) {
        bucketsMap.remove(b.value);
        b.prev.next = b.next;
        b.next.prev = b.prev;
        b.prev = null;
        b.next = null;
    }
}