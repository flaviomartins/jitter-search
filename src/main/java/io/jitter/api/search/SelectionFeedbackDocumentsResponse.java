package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Sets;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonPropertyOrder({"method", "c_sel", "c_r", "sources", "topics", "fbDocs", "fbTerms", "fbFeatures", "fbFeaturesSize", "fbJaccSimilarity", "shardsFV", "feedbackFV", "fbVector", "numFound", "start", "selectDocs", "shardDocs", "docs"})
public class SelectionFeedbackDocumentsResponse extends SelectionSearchDocumentsResponse {

    private int fbDocs;
    private int fbTerms;
    private Map<String, Float> shardsFV;
    private Map<String, Float> feedbackFV;
    private Map<String, Float> fbVector;
    private List<?> docs;
    private Sets.SetView<String> fbFeatures;
    private double fbFeaturesSize;
    private double fbJaccSimilarity;

    public SelectionFeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionFeedbackDocumentsResponse(Set<Map.Entry<String, Double>> sources, Set<Map.Entry<String, Double>> topics, String method, int fbDocs, int fbTerms, Map<String, Float> shardsFV, Map<String, Float> feedbackFV, Map<String, Float> fbVector, int numFound, int start, List<?> selectDocs, List<?> shardDocs) {
        super(sources, topics, method, numFound, start, selectDocs, shardDocs);
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.shardsFV = shardsFV;
        this.feedbackFV = feedbackFV;
        this.fbVector = fbVector;
    }

    public SelectionFeedbackDocumentsResponse(Set<Map.Entry<String, Double>> sources, Set<Map.Entry<String, Double>> topics, String method, int fbDocs, int fbTerms, Map<String, Float> shardsFV, Map<String, Float> feedbackFV, Map<String, Float> fbVector, int start, SelectionTopDocuments selectionTopDocuments, SelectionTopDocuments shardDocuments, TopDocuments topDocuments) {
        super(sources, topics, method, start, selectionTopDocuments, shardDocuments);
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
    public Map<String, Float> getShardsFV() {
        return shardsFV;
    }

    @JsonProperty
    public Map<String, Float> getFeedbackFV() {
        return feedbackFV;
    }

    @JsonProperty
    public Map<String, Float> getFbVector() {
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
