package io.jitter.core.search;

import io.jitter.api.search.Document;

import java.util.List;


public class TopDocuments {

    /** The total number of hits for the query. */
    public final int totalHits;

    /** The top hits for the query. */
    public List<Document> scoreDocs;

    /** Constructs a TopDocuments taking the size from the input */
    public TopDocuments(List<Document> scoreDocs) {
    this(scoreDocs.size(), scoreDocs);
  }

    public TopDocuments(int totalHits, List<Document> scoreDocs) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
    }
  
}

