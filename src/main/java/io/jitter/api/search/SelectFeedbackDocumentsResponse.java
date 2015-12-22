package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"sources", "topics", "method", "selectDocs", "fbDocs", "fbTerms", "numFound", "start", "docs"})
public class SelectFeedbackDocumentsResponse extends SelectDocumentsResponse {

    private Map<String, Double> sources;
    private Map<String, Double> topics;
    private String method;
    private int fbDocs;
    private int fbTerms;
    private int numFound;
    private int start;
    private List<Document> selectDocs;
    private List<?> docs;

    public SelectFeedbackDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectFeedbackDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int fbDocs, int fbTerms, int numFound, int start, List<Document> selectDocs, List<?> docs) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        this.fbDocs = fbDocs;
        this.fbTerms = fbTerms;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = selectDocs;
        this.docs = docs;
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
    public Map<String, Double> getSources() {
        return sources;
    }

    @JsonProperty
    public List<?> getDocs() {
        return docs;
    }

    @JsonProperty
    public String getMethod() {
        return method;
    }

    @JsonProperty
    public Map<String, Double> getTopics() {
        return topics;
    }

    @JsonProperty
    public List<Document> getSelectDocs() {
        return selectDocs;
    }

}
