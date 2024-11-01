package io.jitter.core.search;

import io.jitter.api.search.AbstractDocument;

import java.util.List;


public class TopDocuments {

    /** The total number of hits for the query. */
    public final int totalHits;

    /** The top hits for the query. */
    public List<? extends AbstractDocument> scoreDocs;

    /** Constructs a TopDocuments taking the size from the input */
    public TopDocuments(List<? extends AbstractDocument> scoreDocs) {
    this(scoreDocs.size(), scoreDocs);
  }

    public TopDocuments(int totalHits, List<? extends AbstractDocument> scoreDocs) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
    }

}

