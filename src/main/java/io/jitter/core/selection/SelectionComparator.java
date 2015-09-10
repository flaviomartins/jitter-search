package io.jitter.core.selection;

import java.util.Comparator;
import java.util.Map;

public class SelectionComparator implements Comparator<String> {
    private final Map<String, Double> map;

    public SelectionComparator(Map<String, Double> map) {
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
