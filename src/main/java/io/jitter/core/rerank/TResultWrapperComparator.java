package io.jitter.core.rerank;


import io.jitter.core.twittertools.api.TResultWrapper;

import java.util.Comparator;


public class TResultWrapperComparator implements Comparator<TResultWrapper> {
    private final boolean decreasing;

    @SuppressWarnings("SameParameterValue")
    public TResultWrapperComparator(boolean decreasing) {
        this.decreasing = decreasing;
    }

    public int compare(TResultWrapper x, TResultWrapper y) {
        double xVal = x.getRsv();
        double yVal = y.getRsv();

        if (decreasing) {
            return (xVal > yVal ? -1 : (xVal == yVal ? 0 : 1));
        } else {
            return (xVal < yVal ? -1 : (xVal == yVal ? 0 : 1));
        }

    }

}
