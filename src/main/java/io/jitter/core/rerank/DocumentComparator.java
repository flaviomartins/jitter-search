package io.jitter.core.rerank;

import io.jitter.api.search.IDocument;

import java.util.Comparator;


public class DocumentComparator implements Comparator<IDocument> {
    private final boolean decreasing;

    @SuppressWarnings("SameParameterValue")
    public DocumentComparator(boolean decreasing) {
        this.decreasing = decreasing;
    }

    @Override
    public int compare(IDocument x, IDocument y) {
        double xVal = x.getRsv();
        double yVal = y.getRsv();

        if (decreasing) {
            return (xVal > yVal ? -1 : (xVal == yVal ? 0 : 1));
        } else {
            return (xVal < yVal ? -1 : (xVal == yVal ? 0 : 1));
        }

    }

}
