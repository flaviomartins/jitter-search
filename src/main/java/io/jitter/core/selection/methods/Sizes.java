package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;
import io.jitter.core.shards.ShardStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sizes<E extends ShardedDocument> extends SelectionMethod<E> {

    Sizes() {
    }

    @Override
    public Map<String, Double> rank(List<E> results, ShardStats csiStats) {
        Map<String, Double> sizes = new HashMap<>();
        for (Map.Entry<String, Integer> entry : csiStats.getSizes().entrySet()) {
            sizes.put(entry.getKey(), (double)entry.getValue());
        }
        return sizes;
    }
}
