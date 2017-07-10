package io.jitter.core.selection;

import io.jitter.api.search.StatusDocument;

import java.util.List;


public class SelectionTopDocuments {

    /** The total number of hits for the query. */
    public final int totalHits;

    /** The top hits for the query. */
    public List<StatusDocument> scoreDocs;

    /** The cost of selection for the query. */
    public int c_sel;

    /** The cost of retrieval for the query. */
    public int c_r;

    /** Constructs a TopDocuments taking the size from the input */
    public SelectionTopDocuments(List<StatusDocument> scoreDocs) {
        this(scoreDocs.size(), scoreDocs);
    }

    public SelectionTopDocuments(int totalHits, List<StatusDocument> scoreDocs) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
    }

    public int getC_sel() {
        return c_sel;
    }

    public void setC_sel(int c_sel) {
        this.c_sel = c_sel;
    }

    public int getC_r() {
        return c_r;
    }

    public void setC_r(int c_r) {
        this.c_r = c_r;
    }

    public int getC_res() {
        return c_sel + c_r;
    }
  
}

