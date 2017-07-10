package io.jitter.core.selection.methods;

import io.jitter.api.search.StatusDocument;
import io.jitter.core.shards.ShardStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sizes extends SelectionMethod {

    Sizes() {
    }

    @Override
    public Map<String, Double> rank(List<StatusDocument> results, ShardStats csiStats) {
        Map<String, Double> sizes = new HashMap<>();
        for (Map.Entry<String, Integer> entry : csiStats.getSizes().entrySet()) {
            sizes.put(entry.getKey(), (double)entry.getValue());
        }
        return sizes;
    }

    @Override
    public Map<String, Double> rankTopics(List<StatusDocument> results, ShardStats csiStats, ShardStats shardStats, Map<String, String> reverseTopicMap) {
        Map<String, Double> sizes = new HashMap<>();
        for (Map.Entry<String, Integer> entry : shardStats.getSizes().entrySet()) {
            sizes.put(entry.getKey(), (double)entry.getValue());
        }
        return sizes;
    }

    @Override
    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats csiStats, ShardStats shardStats) {
        return rank;
    }
}
