package io.jitter.core.utils;

import io.jitter.api.search.Document;

import java.util.ArrayList;
import java.util.List;

public class TimeUtils {

    public static List<Double> extractEpochsFromResults(List<Document> results) {
        List<Double> epochs = new ArrayList<>(results.size());
        for (Document result : results) {
            epochs.add((double) result.getEpoch());
        }
        return epochs;
    }

    public static List<Double> adjustEpochsToLandmark(List<Double> epochs, double landmark, double scaleDenominator) {
        List<Double> scaled = new ArrayList<>(epochs.size());
        for (Double rawEpoch : epochs) {
            scaled.add(TimeUtils.adjustEpochToLandmark(rawEpoch, landmark, scaleDenominator));
        }
        return scaled;
    }

    public static double adjustEpochToLandmark(double rawEpoch, double landmark, double scaleDenominator) {
        return (landmark - rawEpoch) / scaleDenominator;
    }
}
