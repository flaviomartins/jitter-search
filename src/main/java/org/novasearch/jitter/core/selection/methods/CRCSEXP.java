package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSEXP extends SelectionMethod {

    public float alpha = 1.2f;
    public float beta = 0.28f;

    protected CRCSEXP() {
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        HashMap<String, Float> map = new HashMap<>();
        int j = 1;
        for (Document result : results) {
            float r = getScore(j);
            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, r);
            } else {
                float cur = map.get(screenName);
                map.put(screenName, cur + r);
            }
            j++;
        }
        return map;
    }

    private float getScore(int j) {
        return (float) (alpha * Math.exp(-beta * j));
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
