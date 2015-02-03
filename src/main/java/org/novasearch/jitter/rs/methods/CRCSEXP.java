package org.novasearch.jitter.rs.methods;

import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.rs.ResourceSelectionMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSEXP extends ResourceSelectionMethod {

    public float alpha = 1.2f;
    public float beta = 0.28f;

    public CRCSEXP() {
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
}
