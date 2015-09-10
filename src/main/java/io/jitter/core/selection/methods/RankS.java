package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankS extends SelectionMethod {
    // Recommended range for the exponent base is [10, 100]
    private final int B;
    private final boolean useScores;

    protected RankS(int B, boolean useScores) {
        this.B = B;
        this.useScores = useScores;
    }

    protected RankS(boolean useScores) {
        this(50, useScores);
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        double minRsv = 0;
        if (useScores) {
            minRsv = getMinRsv(results);
        }

        HashMap<String, Double> map = new HashMap<>();
        int j = 1;
        int step = 1;
        for (Document result : results) {
            double r = getStepFactor(step);
            if (useScores) {
                if (minRsv < 0) {
                    r = r * (result.getRsv() + Math.abs(minRsv));
                } else {
                    r = r * result.getRsv();
                }
            }

            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, r);
            } else {
                double cur = map.get(screenName);
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

    private double getStepFactor(int step) {
        return Math.pow(B, -step);
    }
}
