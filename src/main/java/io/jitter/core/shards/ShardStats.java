package io.jitter.core.shards;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@JsonPropertyOrder({"totalDocs", "maxSize", "sizes"})
public class ShardStats {
    private Set<Map.Entry<String, Integer>> sortedSizes;
    private Map<String, Integer> sizes;
    private int maxSize;
    private int totalDocs;

    public ShardStats(Map<String, Integer> sizes) {
        this.sizes = sizes;
        maxSize = Collections.max(sizes.values());
        totalDocs = 0;
        for (Integer sz : sizes.values()) {
            totalDocs += sz;
        }

        ShardSizeComparator comparator = new ShardSizeComparator(sizes);
        TreeMap<String, Integer> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(sizes);
        sortedSizes = sortedMap.entrySet();
    }

    @JsonProperty("sizes")
    public Set<Map.Entry<String, Integer>> getSortedSizes() {
        return sortedSizes;
    }

    @JsonProperty("sizes")
    public void setSortedSizes(Set<Map.Entry<String, Integer>> sortedSizes) {
        this.sortedSizes = sortedSizes;
    }

    public Map<String, Integer> getSizes() {
        return sizes;
    }

    public void setSizes(Map<String, Integer> sizes) {
        this.sizes = sizes;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getTotalDocs() {
        return totalDocs;
    }

    public void setTotalDocs(int totalDocs) {
        this.totalDocs = totalDocs;
    }
}
