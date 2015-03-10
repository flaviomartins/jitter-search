package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankS extends SelectionMethod {
    // Recommended range for the exponent base is [10, 100]
    public static final int B = 50;
    private boolean useScores = true;

    protected RankS() {
    }

    protected RankS(boolean useScores) {
        this.useScores = useScores;
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        double minRsv = 0;
        if (useScores) {
            minRsv = getMinRsv(results);
        }

        HashMap<String, Float> map = new HashMap<>();
        int j = 1;
        int step = 1;
        for (Document result : results) {
            float r = getStepFactor(step);
            if (useScores) {
                if (minRsv < 0) {
                    r = r * (float)(result.getRsv() + Math.abs(minRsv));
                } else {
                    r = r * (float)(result.getRsv());
                }
            }

            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, r);
            } else {
                float cur = map.get(screenName);
                map.put(screenName, cur + r);
            }

            if (j > 1) {
                step++;
            }
            j++;
        }
        return map;
    }

    private double getMinRsv(List<Document> results) {
        double minRsv = Double.MAX_VALUE;
        for (Document result : results) {
            if (result.getRsv() < minRsv) {
                minRsv = result.getRsv();
            }
        }
        return minRsv;
    }

    private float getStepFactor(int step) {
        return (float) Math.pow(B, -step);
    }
}
