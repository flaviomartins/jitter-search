package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"method", "c_sel", "c_r", "sources", "topics", "fbDocs", "fbTerms", "fbVector", "numFound", "start", "selectDocs", "shardDocs", "docs"})
public class SelectionFeedbackDocumentsResponse extends SelectionSearchDocumentsResponse {

    private int fbDocs;
    private int fbTerms;
    private Map<String, Float> fbVector;
    private List<?> docs;

    public SelectionFeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionFeedbackDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int fbDocs, int fbTerms, Map<String, Float> fbVector, int numFound, int start, List<?> selectDocs, List<?> shardDocs) {
        super(sources, topics, method, numFound, start, selectDocs, shardDocs);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.fbVector = fbVector;
    }

    public SelectionFeedbackDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int fbDocs, int fbTerms, Map<String, Float> fbVector, int start, SelectionTopDocuments selectionTopDocuments, SelectionTopDocuments shardDocuments, TopDocuments topDocuments) {
        super(sources, topics, method, start, selectionTopDocuments, shardDocuments);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.fbVector = fbVector;
        this.numFound = topDocuments.totalHits;
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
    public Map<String, Float> getFbVector() {
        return fbVector;
    }

    @JsonProperty
    public List<?> getDocs() {
        return docs;
    }
}
