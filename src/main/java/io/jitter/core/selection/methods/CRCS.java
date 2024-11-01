package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;

import java.util.HashMap;
import java.util.List;

abstract class CRCS<E extends ShardedDocument> extends SelectionMethod<E> {

    CRCS() {
    }

    HashMap<String, Double> getScores(List<E> results) {
        HashMap<String, Double> scores = new HashMap<>();
        int j = 1;
        for (E result : results) {
            double r = weight(j, results.size());
            String[] shardIds = result.getShardIds();
            if (shardIds != null) {
                for (String shardId : shardIds) {
                    if (!scores.containsKey(shardId)) {
                        scores.put(shardId, r);
                    } else {
                        double cur = scores.get(shardId);
                        scores.put(shardId, cur + r);
                    }
                }
            }
            j++;
        }
        return scores;
    }

    abstract double weight(int j, int size);

}
