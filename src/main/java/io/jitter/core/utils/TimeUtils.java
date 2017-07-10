package io.jitter.core.utils;

import io.jitter.api.search.StatusDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TimeUtils {

    public static List<Double> extractEpochsFromResults(List<StatusDocument> results) {
        List<Double> epochs = new ArrayList<>(results.size());
        epochs.addAll(results.stream().map(result -> (double) result.getEpoch()).collect(Collectors.toList()));
        return epochs;
    }

    public static List<Double> adjustEpochsToLandmark(List<Double> epochs, double landmark, double scaleDenominator) {
        List<Double> scaled = new ArrayList<>(epochs.size());
        scaled.addAll(epochs.stream().map(rawEpoch -> TimeUtils.adjustEpochToLandmark(rawEpoch, landmark, scaleDenominator)).collect(Collectors.toList()));
        return scaled;
    }

    public static double adjustEpochToLandmark(double rawEpoch, double landmark, double scaleDenominator) {
        return (landmark - rawEpoch) / scaleDenominator;
    }
}
