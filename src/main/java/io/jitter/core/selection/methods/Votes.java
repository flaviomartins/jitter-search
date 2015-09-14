package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;
import io.jitter.core.selection.ShardStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Votes extends SelectionMethod {

    Votes() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        return getCounts(results);
    }

    @Override
    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats shardStats) {
        HashMap<String, Double> map = new HashMap<>();
        for (Map.Entry<String, Double> shardScoreEntry : rank.entrySet()) {
            String shardId = shardScoreEntry.getKey().toLowerCase();
            int maxSize = shardStats.getMaxSize();
            int shardSize = shardStats.getSizes().get(shardId);
            double norm = (double) maxSize / shardSize;
            double origScore = shardScoreEntry.getValue();
            double newScore = norm * origScore;
            map.put(shardScoreEntry.getKey(), newScore);
        }
        return map;
    }
}
