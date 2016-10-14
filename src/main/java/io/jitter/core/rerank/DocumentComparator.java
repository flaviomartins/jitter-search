package io.jitter.core.rerank;

import io.jitter.api.search.Document;

import java.util.Comparator;


public class DocumentComparator implements Comparator<Document> {
    private final boolean decreasing;

    @SuppressWarnings("SameParameterValue")
    public DocumentComparator(boolean decreasing) {
        this.decreasing = decreasing;
    }

    @Override
    public int compare(Document x, Document y) {
        double xVal = x.getRsv();
        double yVal = y.getRsv();

        if (decreasing) {
            return (xVal > yVal ? -1 : (xVal == yVal ? 0 : 1));
        } else {
            return (xVal < yVal ? -1 : (xVal == yVal ? 0 : 1));
        }

    }

}
