package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;
import io.jitter.core.shards.ShardStats;

import java.util.List;
import java.util.Map;

public class CRCSEXP extends CRCS {

    private float alpha = 1.2f;
    private float beta = 0.28f;

    CRCSEXP() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results, ShardStats csiStats) {
        return getScores(results);
    }

    @Override
    double weight(int j, int size) {
        return alpha * Math.exp(-beta * j);
    }

    public float getAlpha() {
        return alpha;
    }

    public void setAlpha(float alpha) {
        this.alpha = alpha;
    }

    public float getBeta() {
        return beta;
    }

    public void setBeta(float beta) {
        this.beta = beta;
    }
}
