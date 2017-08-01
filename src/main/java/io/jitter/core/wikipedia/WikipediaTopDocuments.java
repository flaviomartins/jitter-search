package io.jitter.core.wikipedia;

import io.jitter.api.search.AbstractDocument;

import java.util.List;


public class WikipediaTopDocuments {

    /** The total number of hits for the query. */
    public final int totalHits;

    /** The top hits for the query. */
    public List<? extends AbstractDocument> scoreDocs;

    /** Constructs a TopDocuments taking the size from the input */
    public WikipediaTopDocuments(List<? extends AbstractDocument> scoreDocs) {
    this(scoreDocs.size(), scoreDocs);
  }

    public WikipediaTopDocuments(int totalHits, List<? extends AbstractDocument> scoreDocs) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
    }

}

