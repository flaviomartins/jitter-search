package io.jitter.api.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"method", "c_sel", "collections", "numFound", "start", "selectDocs"})
public class SelectionDocumentsResponse {

    private Map<String, Double> collections;
    private String method;
    private int c_sel;
    private int numFound;
    private int start;
    private List<?> selectDocs;

    public SelectionDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionDocumentsResponse(Map<String, Double> collections, String method, int c_sel, int numFound, int start, List<?> selectDocs) {
        this.collections = collections;
        this.method = method;
        this.c_sel = c_sel;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = selectDocs;
    }

    public SelectionDocumentsResponse(Map<String, Double> collections, String method, int start, SelectionTopDocuments topDocuments) {
        this.collections = collections;
        this.c_sel = topDocuments.c_sel;
        this.method = method;
        this.numFound = topDocuments.totalHits;
        this.start = start;
        this.selectDocs = topDocuments.scoreDocs;
    }

    public SelectionDocumentsResponse(Map<String, Double> collections, String method, int c_sel, int numFound, int start) {
        this.collections = collections;
        this.method = method;
        this.c_sel = c_sel;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = null;
    }

    @JsonProperty
    public int getNumFound() {
        return numFound;
    }

    @JsonProperty
    public int getStart() {
        return start;
    }

    @JsonProperty
    public Map<String, Double> getCollections() {
        return collections;
    }

    @JsonProperty
    public List<?> getSelectDocs() {
        return selectDocs;
    }

    @JsonProperty
    public String getMethod() {
        return method;
    }

    @JsonProperty
    public int getC_sel() {
        return c_sel;
    }
}
