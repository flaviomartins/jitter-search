package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DocumentsResponse {

    private int numFound;
    private int start;
    private List<Document> docs;

    public DocumentsResponse() {
        // Jackson deserialization
    }

    public DocumentsResponse(int numFound, int start, List<Document> docs) {
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
    public List<Document> getDocs() {
        return docs;
    }
}
