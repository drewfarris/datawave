package datawave.util;

import com.google.common.collect.Sets;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class MinHash {
    private final TreeMap<Long,String> contents;
    private final int maxSize;
    
    public MinHash(int hashCount) {
        this.contents = new TreeMap<>();
        this.maxSize = hashCount;
    }
    
    public int getHashCount() {
        return maxSize;
    }
    
    public void add(long hashValue, String name) {
        contents.put(hashValue, name);
        while (contents.size() > maxSize) {
            contents.remove(contents.lastKey());
        }
    }
    
    public void add(byte[] item, String name) {
        add(hash(item), name);
    }
    
    public Long hash(byte[] item) {
        // TODO: find something better than FNV-1a
        long hash = 2166136261L;
        for (final byte b : item) {
            hash = hash ^ (b & 0xff);
            hash = hash * 16777619;
            hash = hash ^ (b >> 16);
            hash = hash * 16777619;
        }
        return hash;
    }
    
    public boolean isSaturated() {
        return contents.size() >= maxSize;
    }
    
    /**
     * Fetch an unmodifiable view of the contents mapping
     */
    public Map<Long,String> getContents() {
        return Collections.unmodifiableMap(contents);
    }
    
    public static MinHash union(Collection<MinHash> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException();
        }
        
        int newMaxSize = input.stream().map(s -> s.maxSize).min(Comparator.naturalOrder()).get();
        MinHash result = new MinHash(newMaxSize);
        input.forEach(s -> result.contents.putAll(s.contents));
        while (result.contents.size() > result.maxSize) {
            result.contents.remove(result.contents.lastKey());
        }
        return result;
    }
    
    public static MinHash union(MinHash... input) {
        return union(Arrays.asList(input));
    }
    
    /**
     * Approximate the Jaccard similarity of the sets represented by {@code left} and {@code right}.
     */
    public static double similarity(MinHash left, MinHash right) {
        if (left.contents.isEmpty() || right.contents.isEmpty()) {
            return left.contents.isEmpty() && right.contents.isEmpty() ? 1.0 : 0.0;
        }
        int maxSize = Math.max(left.contents.size(), right.contents.size());
        
        TreeSet<Long> leftTrim = new TreeSet<>(left.contents.keySet());
        while (leftTrim.size() > maxSize) {
            leftTrim.remove(leftTrim.last());
        }
        
        TreeSet<Long> rightTrim = new TreeSet<>(right.contents.keySet());
        while (rightTrim.size() > maxSize) {
            rightTrim.remove(rightTrim.last());
        }
        
        TreeSet<Long> sample = new TreeSet<>();
        sample.addAll(leftTrim);
        sample.addAll(rightTrim);
        while (sample.size() > maxSize) {
            sample.remove(sample.last());
        }
        sample.retainAll(leftTrim);
        sample.retainAll(rightTrim);
        return sample.size() / (double) maxSize;
    }
    
    public static Set<String> sampleDifferences(MinHash left, MinHash right) {
        if (left.isSaturated() || right.isSaturated()) {
            if (left.contents.isEmpty())
                return new TreeSet<>(right.contents.values());
            if (right.contents.isEmpty())
                return new TreeSet<>(left.contents.values());
            TreeSet<String> result = new TreeSet<>();
            
            int minSize = Math.min(left.maxSize, right.maxSize);
            
            TreeSet<Long> leftTrim = new TreeSet<>(left.contents.keySet());
            while (leftTrim.size() > minSize) {
                leftTrim.remove(leftTrim.last());
            }
            
            TreeSet<Long> rightTrim = new TreeSet<>(right.contents.keySet());
            while (rightTrim.size() > minSize) {
                rightTrim.remove(rightTrim.last());
            }
            
            TreeSet<Long> sample = new TreeSet<>();
            sample.addAll(leftTrim);
            sample.addAll(rightTrim);
            while (sample.size() > minSize) {
                sample.remove(sample.last());
            }
            
            for (long key : Sets.difference(sample, rightTrim)) {
                result.add(left.contents.get(key));
            }
            for (long key : Sets.difference(sample, leftTrim)) {
                result.add(right.contents.get(key));
            }
            return result;
        } else {
            Set<String> result = new HashSet<>();
            for (long key : Sets.union(left.contents.keySet(), right.contents.keySet())) {
                if (!left.contents.containsKey(key)) {
                    result.add(right.contents.get(key));
                } else if (!right.contents.containsKey(key)) {
                    result.add(left.contents.get(key));
                }
            }
            return result;
        }
    }
    
    public void writeTo(OutputStream os) throws IOException {
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeInt(maxSize);
        dos.writeInt(contents.size());
        for (Map.Entry<Long,String> e : contents.entrySet()) {
            dos.writeLong(e.getKey());
            byte[] s = e.getValue().getBytes(StandardCharsets.UTF_8);
            dos.writeShort(s.length);
            dos.write(s);
        }
    }
    
    public static MinHash readFrom(InputStream is) throws IOException {
        DataInputStream dis = new DataInputStream(is);
        int maxSize = dis.readInt();
        MinHash result = new MinHash(maxSize);
        int contentSize = dis.readInt();
        for (int i = 0; i < contentSize; i++) {
            long h = dis.readLong();
            short len = dis.readShort();
            byte[] s = new byte[len];
            dis.readFully(s);
            result.contents.put(h, new String(s, StandardCharsets.UTF_8));
        }
        return result;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        MinHash minHash = (MinHash) o;
        
        if (maxSize != minHash.maxSize)
            return false;
        return contents.equals(minHash.contents);
    }
    
    @Override
    public int hashCode() {
        int result = contents.hashCode();
        result = 31 * result + maxSize;
        return result;
    }
}
