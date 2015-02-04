package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.core.selection.SelectionMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSISR extends SelectionMethod {

    public CRCSISR() {
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
        return isr;
    }

    private float getScore(int j) {
        return (float) (1.0 / (Math.pow(j, 2)));
    }

}
