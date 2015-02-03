package org.novasearch.jitter.rs;

import org.novasearch.jitter.api.search.Document;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class ResourceSelectionMethod {

    protected ResourceSelectionMethod() {
    }

    public SortedMap<String, Float> getRanked(List<Document> results) {
        Map<String, Float> map = rank(results);
        return getSortedMap(map);
    }

    private SortedMap<String, Float> getSortedMap(Map<String, Float> map) {
        ResourceSelectionComparator comparator = new ResourceSelectionComparator(map);
        TreeMap<String, Float> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(map);
        return sortedMap;
    }

    public abstract Map<String, Float> rank(List<Document> results);

}
