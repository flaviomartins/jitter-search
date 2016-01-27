package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;
import io.jitter.core.shards.ShardStats;

import java.util.*;

public abstract class SelectionMethod {

    SelectionMethod() {
    }

    Map<String, Double> getCounts(List<Document> results) {
        Map<String, Double> counts = new HashMap<>();
        for (Document result : results) {
            String screenName = result.getScreen_name();
            if (!counts.containsKey(screenName)) {
                counts.put(screenName, 1d);
            } else {
                double cur = counts.get(screenName);
                counts.put(screenName, cur + 1d);
            }
        }
        return counts;
    }

    public abstract Map<String, Double> rank(List<Document> results);

    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats csiStats, ShardStats shardStats) {
        double c_max = 1;
        for (String shardId : rank.keySet()) {
            String shardIdLower = shardId.toLowerCase(Locale.ROOT);
            if (shardStats.getSizes().containsKey(shardIdLower)) {
                int sz = shardStats.getSizes().get(shardIdLower);
                if (sz > c_max)
                    c_max = sz;
            }
        }

        HashMap<String, Double> map = new HashMap<>();
        for (Map.Entry<String, Double> shardScoreEntry : rank.entrySet()) {
            String shardIdLower = shardScoreEntry.getKey().toLowerCase(Locale.ROOT);
            if (shardStats.getSizes().containsKey(shardIdLower)) {
                double c_i = shardStats.getSizes().get(shardIdLower);
                double s_i = csiStats.getSizes().get(shardIdLower);
                double norm = (1.0 / c_max) * (c_i / s_i);
                double origScore = shardScoreEntry.getValue();
                double newScore = norm * origScore;
                map.put(shardScoreEntry.getKey(), newScore);
            }
        }
        return map;
    }

}
