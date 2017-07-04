package io.jitter.core.wikipedia;

import io.jitter.api.wikipedia.WikipediaDocument;

import java.util.List;


public class WikipediaTopDocuments {

    /** The total number of hits for the query. */
    public final int totalHits;

    /** The top hits for the query. */
    public List<WikipediaDocument> scoreDocs;

    /** Constructs a TopDocuments taking the size from the input */
    public WikipediaTopDocuments(List<WikipediaDocument> scoreDocs) {
    this(scoreDocs.size(), scoreDocs);
  }

    public WikipediaTopDocuments(int totalHits, List<WikipediaDocument> scoreDocs) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
    }

}

