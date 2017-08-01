package io.jitter.core.selection.methods;

import io.jitter.api.search.AbstractDocument;

import java.util.HashMap;
import java.util.List;

abstract class CRCS extends SelectionMethod {

    CRCS() {
    }

    HashMap<String, Double> getScores(List<? extends AbstractDocument> results) {
        HashMap<String, Double> scores = new HashMap<>();
        int j = 1;
        for (AbstractDocument result : results) {
            double r = weight(j, results.size());
            String[] shardIds = result.getShardIds();
            for (String shardId : shardIds) {
                if (!scores.containsKey(shardId)) {
                    scores.put(shardId, r);
                } else {
                    double cur = scores.get(shardId);
                    scores.put(shardId, cur + r);
                }
            }
            j++;
        }
        return scores;
    }

    abstract double weight(int j, int size);

}
