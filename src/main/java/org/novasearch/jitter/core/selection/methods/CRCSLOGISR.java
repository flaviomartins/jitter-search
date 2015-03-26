package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSLOGISR extends SelectionMethod {

    protected CRCSLOGISR() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        HashMap<String, Double> isr = new HashMap<>();

        // sum isr
        int j = 1;
        for (Document result : results) {
            double r = getScore(j);
            String screenName = result.getScreen_name();
            if (!isr.containsKey(screenName)) {
                isr.put(screenName, r);
            } else {
                double cur = isr.get(screenName);
                isr.put(screenName, cur + r);
            }
            j++;
        }

        Map<String, Double> counts = getCounts(results);

        // log multiplication
        for (Map.Entry<String, Double> entry : isr.entrySet()) {
            double count = counts.get(entry.getKey());
            isr.put(entry.getKey(), Math.log(1.0 + count) * entry.getValue());
        }
        return isr;
    }

    private double getScore(int j) {
        return 1.0 / Math.pow(j, 2);
    }

}
