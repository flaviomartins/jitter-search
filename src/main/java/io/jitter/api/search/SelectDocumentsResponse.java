package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"numFound", "start", "method", "sources", "topics", "selectDocs", "docs"})
public class SelectDocumentsResponse {

    private Map<String, Double> sources;
    private Map<String, Double> topics;
    private String method;
    private int numFound;
    private int start;
    private List<?> selectDocs;
    private List<?> docs;

    public SelectDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int numFound, int start, List<Document> selectDocs, List<?> docs) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = selectDocs;
        this.docs = docs;
    }

    public SelectDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int numFound, int start, TopDocuments selectTopDocuments, TopDocuments topDocuments) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = selectTopDocuments.scoreDocs;
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
    public List<?> getSelectDocs() {
        return selectDocs;
    }
}
