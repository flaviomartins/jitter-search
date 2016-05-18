package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"numFound", "start", "fbDocs", "fbTerms", "fbVector", "docs"})
public class FeedbackDocumentsResponse extends DocumentsResponse {

    private int fbDocs;
    private int fbTerms;
    private Map<String, Float> fbVector;
    private int numFound;
    private int start;
    private List<?> docs;

    public FeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public FeedbackDocumentsResponse(int fbDocs, int fbTerms, Map<String, Float> fbVector, int numFound, int start, List<Document> docs) {
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.fbVector = fbVector;
        this.numFound = numFound;
        this.start = start;
        this.docs = docs;
    }

    public FeedbackDocumentsResponse(int fbDocs, int fbTerms, Map<String, Float> fbVector, int start, TopDocuments topDocuments) {
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.fbVector = fbVector;
        this.start = start;
        if (topDocuments != null) {
            this.numFound = topDocuments.totalHits;
            this.docs = topDocuments.scoreDocs;
        }
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
    public Map<String, Float> getFbVector() {
        return fbVector;
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
