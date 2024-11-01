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

    public FeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public FeedbackDocumentsResponse(int fbDocs, int fbTerms, Map<String, Float> fbVector, int numFound, int start, List<StatusDocument> docs) {
        super(numFound, start, docs);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.fbVector = fbVector;
    }

    public FeedbackDocumentsResponse(int fbDocs, int fbTerms, Map<String, Float> fbVector, int start, TopDocuments topDocuments) {
        super(topDocuments.totalHits, start, topDocuments.scoreDocs);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.fbVector = fbVector;
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

}
