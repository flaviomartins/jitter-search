package io.jitter.core.selection.methods;

import io.jitter.api.search.StatusDocument;
import io.jitter.core.shards.ShardStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class SelectionMethod {

    private static final Logger logger = LoggerFactory.getLogger(SelectionMethod.class);


    SelectionMethod() {
    }

    Map<String, Double> getCounts(List<StatusDocument> results) {
        Map<String, Double> counts = new HashMap<>();
        for (StatusDocument result : results) {
            String shardId = result.getShardId();
            if (!counts.containsKey(shardId)) {
                counts.put(shardId, 1d);
            } else {
                double cur = counts.get(shardId);
                counts.put(shardId, cur + 1d);
            }
        }
        return counts;
    }

    public abstract Map<String, Double> rank(List<StatusDocument> results, ShardStats csiStats);

    public Map<String, Double> rankTopics(List<StatusDocument> results, ShardStats csiStats, ShardStats shardStats, Map<String, String> reverseTopicMap) {
        Map<String, Double> rankedCollections = rank(results, csiStats);
        Map<String, Double> rankedTopics = new HashMap<>();
        for (String col : rankedCollections.keySet()) {
            if (reverseTopicMap.containsKey(col.toLowerCase(Locale.ROOT))) {
                String topic = reverseTopicMap.get(col.toLowerCase(Locale.ROOT)).toLowerCase(Locale.ROOT);
                double cur = 0;

                if (rankedTopics.containsKey(topic))
                    cur = rankedTopics.get(topic);
                else
                    rankedTopics.put(topic, 0d);

                double sum = cur + rankedCollections.get(col);
                rankedTopics.put(topic, sum);
            } else {
                logger.warn("{} not mapped to a topic!", col);
            }
        }
        return rankedTopics;
    }

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
