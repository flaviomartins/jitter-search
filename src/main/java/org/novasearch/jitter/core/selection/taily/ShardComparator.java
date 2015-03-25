package org.novasearch.jitter.core.selection.taily;

import java.util.Comparator;
import java.util.Map;

public class ShardComparator implements Comparator<String> {
    final Map<String, Double> map;
    public ShardComparator(Map<String, Double> map) {
        this.map = map;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.
    public int compare(String a, String b) {
        if (map.get(a) >= map.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
