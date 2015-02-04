package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSLIN extends SelectionMethod {

    protected CRCSLIN() {
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        HashMap<String, Float> map = new HashMap<>();
        int gamma = results.size();
        int j = 1;
        for (Document result : results) {
            float r = getScore(gamma, j);
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

    private float getScore(int gamma, int j) {
        return gamma - j;
    }
}
