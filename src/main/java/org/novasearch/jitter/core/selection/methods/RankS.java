package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankS extends SelectionMethod {

    protected RankS() {
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        HashMap<String, Float> map = new HashMap<>();
        int j = 1;
        int level = 1;
        for (Document result : results) {
            float r = getScore(level);
            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, r);
            } else {
                float cur = map.get(screenName);
                map.put(screenName, cur + r);
            }
            if (j > 1) {
                level++;
            }
            j++;
        }
        return map;
    }

    private float getScore(int level) {
        // B = 10 [2, 100]
        return (float) (1.0 * Math.pow(10, -level));
    }
}
