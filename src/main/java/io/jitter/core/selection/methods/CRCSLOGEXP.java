package io.jitter.core.selection.methods;

import io.jitter.api.search.AbstractDocument;
import io.jitter.core.shards.ShardStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSLOGEXP extends CRCSEXP {

    CRCSLOGEXP() {
    }

    @Override
    public Map<String, Double> rank(List<? extends AbstractDocument> results, ShardStats csiStats) {
        HashMap<String, Double> scores = getScores(results);
        Map<String, Double> counts = getCounts(results);

        // log multiplication
        for (Map.Entry<String, Double> entry : scores.entrySet()) {
            double count = counts.get(entry.getKey());
            scores.put(entry.getKey(), Math.log(1.0 + count) * entry.getValue());
        }
        return scores;
    }

}
