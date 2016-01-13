package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;

import java.util.List;

@JsonPropertyOrder({"numFound", "start", "fbDocs", "fbTerms", "docs"})
public class FeedbackDocumentsResponse extends DocumentsResponse {

    private int fbDocs;
    private int fbTerms;
    private int numFound;
    private int start;
    private List<?> docs;

    public FeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public FeedbackDocumentsResponse(int fbDocs, int fbTerms, int numFound, int start, List<Document> docs) {
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.numFound = numFound;
        this.start = start;
        this.docs = docs;
    }

    public FeedbackDocumentsResponse(int fbDocs, int fbTerms, int numFound, int start, TopDocuments topDocuments) {
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.numFound = numFound;
        this.start = start;
        this.docs = topDocuments.scoreDocs;
    }

    @JsonProperty
    public int getFbDocs() {
        return fbDocs;
    }

    @JsonProperty
    public int getFbTerms() {
        return fbTerms;
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
