package org.novasearch.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class SelectDocumentsResponse {

    private Map<String, Double> collections;
    private String method;
    private int numFound;
    private int start;
    private List<Document> docs;

    public SelectDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectDocumentsResponse(Map<String, Double> collections, String method, int numFound, int start, List<Document> docs) {
        this.collections = collections;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.docs = docs;
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
    public List<Document> getDocs() {
        return docs;
    }

    @JsonProperty
    public String getMethod() {
        return method;
    }
}
