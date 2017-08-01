package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;
import io.jitter.core.shards.ShardStats;

import java.util.List;
import java.util.Map;

public class CRCSLIN extends CRCS {

    CRCSLIN() {
    }

    @Override
    public Map<String, Double> rank(List<ShardedDocument> results, ShardStats csiStats) {
        return getScores(results);
    }

    @Override
    double weight(int j, int size) {
        return size - j;
    }
}
