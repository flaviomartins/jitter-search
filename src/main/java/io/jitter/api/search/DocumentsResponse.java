package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;

import java.util.List;

@JsonPropertyOrder({"numFound", "start", "docs"})
public class DocumentsResponse {

    private int numFound;
    private int start;
    private List<?> docs;

    public DocumentsResponse() {
        // Jackson deserialization
    }

    public DocumentsResponse(int numFound, int start, List<?> docs) {
        this.numFound = numFound;
        this.start = start;
        this.docs = docs;
    }

    public DocumentsResponse(int numFound, int start, TopDocuments topDocuments) {
        this.numFound = numFound;
        this.start = start;
        this.docs = topDocuments.scoreDocs;
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
    public List<?> getDocs() {
        return docs;
    }
}
