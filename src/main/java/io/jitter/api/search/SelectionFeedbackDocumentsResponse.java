package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Sets;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonPropertyOrder({"method", "c_sel", "c_r", "collections", "fbDocs", "fbTerms", "fbFeatures", "fbFeaturesSize", "fbJaccSimilarity", "shardsFV", "feedbackFV", "fbVector", "numFound", "start", "selectDocs", "shardDocs", "docs"})
public class SelectionFeedbackDocumentsResponse extends SelectionSearchDocumentsResponse {

    private int fbDocs;
    private int fbTerms;
    private Set<Map.Entry<String, Float>> shardsFV;
    private Set<Map.Entry<String, Float>> feedbackFV;
    private Set<Map.Entry<String, Float>> fbVector;
    private List<?> docs;
    private Sets.SetView<String> fbFeatures;
    private double fbFeaturesSize;
    private double fbJaccSimilarity;

    public SelectionFeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionFeedbackDocumentsResponse(Set<Map.Entry<String, Double>> collections, String method, int fbDocs, int fbTerms, Set<Map.Entry<String, Float>> shardsFV, Set<Map.Entry<String, Float>> feedbackFV, Set<Map.Entry<String, Float>> fbVector, int numFound, int start, List<?> selectDocs, List<?> shardDocs) {
        super(collections, method, numFound, start, selectDocs, shardDocs);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.shardsFV = shardsFV;
        this.feedbackFV = feedbackFV;
        this.fbVector = fbVector;
    }

    public SelectionFeedbackDocumentsResponse(Set<Map.Entry<String, Double>> collections, String method, int fbDocs, int fbTerms, Set<Map.Entry<String, Float>> shardsFV, Set<Map.Entry<String, Float>> feedbackFV, Set<Map.Entry<String, Float>> fbVector, int start, SelectionTopDocuments selectionTopDocuments, SelectionTopDocuments shardDocuments, TopDocuments topDocuments) {
        super(collections, method, start, selectionTopDocuments, shardDocuments);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.shardsFV = shardsFV;
        this.feedbackFV = feedbackFV;
        this.fbVector = fbVector;
        if (topDocuments != null) {
            this.numFound = topDocuments.totalHits;
            this.docs = topDocuments.scoreDocs;
        }
    }

    public SelectionFeedbackDocumentsResponse(Set<Map.Entry<String, Double>> collections, String method, int c_sel, int fbDocs, int fbTerms, Set<Map.Entry<String, Float>> shardsFV, Set<Map.Entry<String, Float>> feedbackFV, Set<Map.Entry<String, Float>> fbVector, int start, List<?> selectDocs, SelectionTopDocuments shardDocuments, TopDocuments topDocuments) {
        super(collections, method, c_sel, start, selectDocs, shardDocuments);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.shardsFV = shardsFV;
        this.feedbackFV = feedbackFV;
        this.fbVector = fbVector;
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
    public Set<Map.Entry<String, Float>> getShardsFV() {
        return shardsFV;
    }

    @JsonProperty
    public Set<Map.Entry<String, Float>> getFeedbackFV() {
        return feedbackFV;
    }

    @JsonProperty
    public Set<Map.Entry<String, Float>> getFbVector() {
        return fbVector;
    }

    @JsonProperty
    public List<?> getDocs() {
        return docs;
    }

    public void setFbFeatures(Sets.SetView<String> fbFeatures) {
        this.fbFeatures = fbFeatures;
    }

    @JsonProperty
    public Sets.SetView<String> getFbFeatures() {
        return fbFeatures;
    }

    public void setFbFeaturesSize(double fbFeaturesSize) {
        this.fbFeaturesSize = fbFeaturesSize;
    }

    @JsonProperty
    public double getFbFeaturesSize() {
        return fbFeaturesSize;
    }

    public void setFbJaccSimilarity(double fbJaccSimilarity) {
        this.fbJaccSimilarity = fbJaccSimilarity;
    }

    @JsonProperty
    public double getFbJaccSimilarity() {
        return fbJaccSimilarity;
    }
}
