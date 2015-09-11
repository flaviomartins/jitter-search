package io.jitter.core.selection;

import java.util.Collections;
import java.util.Map;

public class ShardStats {
    private Map<String, Integer> sizes;
    private int maxSize;
    private int totalDocs;

    public ShardStats(Map<String, Integer> sizes) {
        this.sizes = sizes;
        maxSize = Collections.max(sizes.values());
        totalDocs = 0;
        for (Integer sz : sizes.values()) {
            totalDocs += sz.intValue();
        }
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
