package io.jitter.core.rerank;


import io.jitter.api.wikipedia.WikipediaDocument;

import java.util.Comparator;


public class WikipediaDocumentComparator implements Comparator<WikipediaDocument> {
    private final boolean decreasing;

    @SuppressWarnings("SameParameterValue")
    public WikipediaDocumentComparator(boolean decreasing) {
        this.decreasing = decreasing;
    }

    @Override
    public int compare(WikipediaDocument x, WikipediaDocument y) {
        double xVal = x.getRsv();
        double yVal = y.getRsv();

        if (decreasing) {
            return (xVal > yVal ? -1 : (xVal == yVal ? 0 : 1));
        } else {
            return (xVal < yVal ? -1 : (xVal == yVal ? 0 : 1));
        }

    }

}
