package io.jitter.core.selection.methods;

import io.jitter.core.selection.ShardStats;

import java.util.HashMap;
import java.util.Map;

public abstract class CRCS extends SelectionMethod {

    CRCS() {
    }

    @Override
    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats shardStats) {
        HashMap<String, Double> map = new HashMap<>();
        for (Map.Entry<String, Double> shardScoreEntry : rank.entrySet()) {
            double norm = (double) shardStats.getMaxSize() / shardStats.getSizes().get(shardScoreEntry.getKey().toLowerCase());
            map.put(shardScoreEntry.getKey(), norm * shardScoreEntry.getValue());
        }
        return map;
    }

}
