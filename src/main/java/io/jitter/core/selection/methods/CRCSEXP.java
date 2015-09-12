package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSEXP extends CRCS {

    private float alpha = 1.2f;
    private float beta = 0.28f;

    CRCSEXP() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        HashMap<String, Double> map = new HashMap<>();
        int j = 1;
        for (Document result : results) {
            double r = getScore(j);
            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, r);
            } else {
                double cur = map.get(screenName);
                map.put(screenName, cur + r);
            }
            j++;
        }
        return map;
    }

    private double getScore(int j) {
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
