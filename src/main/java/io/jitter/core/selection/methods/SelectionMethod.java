package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;
import io.jitter.core.shards.ShardStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SelectionMethod<E extends ShardedDocument> {

    private static final Logger logger = LoggerFactory.getLogger(SelectionMethod.class);


    SelectionMethod() {
    }

    Map<String, Double> getCounts(List<E> results) {
        Map<String, Double> counts = new HashMap<>();
        for (ShardedDocument result : results) {
            String[] shardIds = result.getShardIds();
            if (shardIds != null) {
                for (String shardId : shardIds) {
                    if (!counts.containsKey(shardId)) {
                        counts.put(shardId, 1d);
                    } else {
                        double cur = counts.get(shardId);
                        counts.put(shardId, cur + 1d);
                    }
                }
            }
        }
        return counts;
    }

    public abstract Map<String, Double> rank(List<E> results, ShardStats csiStats);

    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats csiStats, ShardStats shardStats) {
        double c_max = 1;
        Set<String> shardIds = rank.keySet();
        for (String shardId : shardIds) {
            if (shardStats.getSizes().containsKey(shardId)) {
                int sz = shardStats.getSizes().get(shardId);
                if (sz > c_max)
                    c_max = sz;
            }
        }

        HashMap<String, Double> map = new HashMap<>();
        for (Map.Entry<String, Double> shardScoreEntry : rank.entrySet()) {
            String shardId = shardScoreEntry.getKey();
            if (shardStats.getSizes().containsKey(shardId)) {
                double c_i = shardStats.getSizes().get(shardId);
                double s_i = csiStats.getSizes().get(shardId);
                double norm = (1.0 / c_max) * (c_i / s_i);
                double origScore = shardScoreEntry.getValue();
                double newScore = norm * origScore;
                map.put(shardScoreEntry.getKey(), newScore);
            }
        }
        return map;
    }

}
