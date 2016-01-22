package io.jitter.core.selection;

import io.jitter.api.search.Document;
import io.jitter.core.search.TopDocuments;

import java.util.List;


public class SelectionTopDocuments extends TopDocuments {

    /** The cost of selection for the query. */
    public int c_sel;

    /** The cost of retrieval for the query. */
    public int c_r;

    /** Constructs a TopDocuments taking the size from the input */
    public SelectionTopDocuments(List<Document> scoreDocs) {
        this(scoreDocs.size(), scoreDocs);
    }

    public SelectionTopDocuments(int totalHits, List<Document> scoreDocs) {
        super(totalHits, scoreDocs);
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

