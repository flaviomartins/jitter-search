package io.jitter.core.shards;

import java.util.Comparator;
import java.util.Map;

public class ShardSizeComparator implements Comparator<String> {
    private final Map<String, Integer> map;

    public ShardSizeComparator(Map<String, Integer> map) {
        this.map = map;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.
    @Override
    public int compare(String a, String b) {
        if (map.get(a) >= map.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
