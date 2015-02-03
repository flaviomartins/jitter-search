package org.novasearch.jitter.rs.methods;

import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.rs.ResourceSelectionMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSLOGISR extends ResourceSelectionMethod {

    public CRCSLOGISR() {
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        HashMap<String, Float> isr = new HashMap<>();

        // sum isr
        int j = 1;
        for (Document result : results) {
            float r = getScore(j);
            String screenName = result.getScreen_name();
            if (!isr.containsKey(screenName)) {
                isr.put(screenName, r);
            } else {
                float cur = isr.get(screenName);
                isr.put(screenName, cur + r);
            }
            j++;
        }

        Map<String, Float> counts = getCounts(results);

        // log multiplication
        for (Map.Entry<String, Float> entry : isr.entrySet()) {
            float count = counts.get(entry.getKey());
            isr.put(entry.getKey(), (float) Math.log(1.0 + count) * entry.getValue());
        }
        return isr;
    }

    private float getScore(int j) {
        return (float) (1.0 / (Math.pow(j, 2)));
    }

}
