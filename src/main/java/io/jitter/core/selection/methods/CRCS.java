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
            String shardId = shardScoreEntry.getKey().toLowerCase();
            int maxSize = shardStats.getMaxSize();
            int sz = shardStats.getSizes().get(shardId);
            double norm = (double) maxSize / sz;
            double origScore = shardScoreEntry.getValue();
            double newScore = norm * origScore;
            map.put(shardScoreEntry.getKey(), newScore);
        }
        return map;
    }

}
