package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSLIN extends SelectionMethod {

    protected CRCSLIN() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        HashMap<String, Double> map = new HashMap<>();
        int gamma = results.size();
        int j = 1;
        for (Document result : results) {
            double r = getScore(gamma, j);
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

    private double getScore(int gamma, int j) {
        return gamma - j;
    }
}
