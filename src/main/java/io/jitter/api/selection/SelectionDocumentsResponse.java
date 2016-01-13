package io.jitter.api.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.core.search.TopDocuments;

import java.util.List;
import java.util.Map;

public class SelectionDocumentsResponse {

    private Map<String, Double> collections;
    private String method;
    private int numFound;
    private int start;
    private List<?> docs;

    public SelectionDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionDocumentsResponse(Map<String, Double> collections, String method, int numFound, int start, List<?> docs) {
        this.collections = collections;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.docs = docs;
    }

    public SelectionDocumentsResponse(Map<String, Double> collections, String method, int numFound, int start, TopDocuments topDocuments) {
        this.collections = collections;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.docs = topDocuments.scoreDocs;
    }

    public SelectionDocumentsResponse(Map<String, Double> collections, String method, int numFound, int start) {
        this.collections = collections;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.docs = null;
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
    public List<?> getDocs() {
        return docs;
    }

    @JsonProperty
    public String getMethod() {
        return method;
    }
}
