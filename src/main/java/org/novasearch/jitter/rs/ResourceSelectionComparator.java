package org.novasearch.jitter.rs;

import java.util.Comparator;
import java.util.Map;

public class ResourceSelectionComparator implements Comparator<String> {
    final Map<String, Float> map;
    public ResourceSelectionComparator(Map<String, Float> map) {
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
