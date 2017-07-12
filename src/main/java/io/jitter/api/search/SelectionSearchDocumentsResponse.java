package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonPropertyOrder({"method", "c_sel", "c_r", "sources", "topics", "numFound", "start", "selectDocs", "docs"})
public class SelectionSearchDocumentsResponse {

    private Set<Map.Entry<String, Double>> sources;
    private Set<Map.Entry<String, Double>> topics;
    private String method;
    private int c_sel;
    private int c_r;
    protected int numFound;
    private int start;
    private List<?> selectDocs;
    private List<?> shardDocs;

    public SelectionSearchDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionSearchDocumentsResponse(Set<Map.Entry<String, Double>> sources, Set<Map.Entry<String, Double>> topics, String method, int numFound, int start, List<?> selectDocs, List<?> shardDocs) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = selectDocs;
        this.shardDocs = shardDocs;
    }

    public SelectionSearchDocumentsResponse(Set<Map.Entry<String, Double>> sources, Set<Map.Entry<String, Double>> topics, String method, int start, SelectionTopDocuments selectionTopDocuments, SelectionTopDocuments shardTopDocuments) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        if (selectionTopDocuments != null) {
            this.selectDocs = selectionTopDocuments.scoreDocs;
            this.c_sel = selectionTopDocuments.getC_sel();
        }
        if (shardTopDocuments != null) {
            this.numFound = shardTopDocuments.totalHits;
            this.shardDocs = shardTopDocuments.scoreDocs;
            this.c_r = shardTopDocuments.getC_r();
        }
        this.start = start;
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
    public Set<Map.Entry<String, Double>> getSources() {
        return sources;
    }

    @JsonProperty
    public List<?> getShardDocs() {
        return shardDocs;
    }

    @JsonProperty
    public String getMethod() {
        return method;
    }

    @JsonProperty
    public Set<Map.Entry<String, Double>> getTopics() {
        return topics;
    }

    @JsonProperty
    public List<?> getSelectDocs() {
        return selectDocs;
    }

    @JsonProperty
    public int getC_sel() {
        return c_sel;
    }

    @JsonProperty
    public int getC_r() {
        return c_r;
    }
}
