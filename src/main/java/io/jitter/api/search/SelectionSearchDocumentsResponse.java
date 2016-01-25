package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"method", "c_sel", "c_r", "sources", "topics", "numFound", "start", "selectDocs", "docs"})
public class SelectionSearchDocumentsResponse {

    private Map<String, Double> sources;
    private Map<String, Double> topics;
    private String method;
    private int c_sel;
    private int c_r;
    private int numFound;
    private int start;
    private List<?> selectDocs;
    private List<?> docs;

    public SelectionSearchDocumentsResponse() {
        // Jackson deserialization
    }

    public SelectionSearchDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int numFound, int start, List<?> selectDocs, List<?> docs) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        this.numFound = numFound;
        this.start = start;
        this.selectDocs = selectDocs;
        this.docs = docs;
    }

    public SelectionSearchDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int start, SelectionTopDocuments selectionTopDocuments, TopDocuments topDocuments) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        if (selectionTopDocuments != null) {
            this.selectDocs = selectionTopDocuments.scoreDocs;
            this.c_sel = selectionTopDocuments.getC_sel();
        }
        if (topDocuments != null) {
            this.numFound = topDocuments.totalHits;
            this.docs = topDocuments.scoreDocs;
        }
        this.start = start;
    }

    public SelectionSearchDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int start, SelectionTopDocuments selectionTopDocuments, SelectionTopDocuments topDocuments) {
        this.sources = sources;
        this.topics = topics;
        this.method = method;
        if (selectionTopDocuments != null) {
            this.selectDocs = selectionTopDocuments.scoreDocs;
            this.c_sel = selectionTopDocuments.getC_sel();
        }
        if (topDocuments != null) {
            this.numFound = topDocuments.totalHits;
            this.docs = topDocuments.scoreDocs;
            this.c_r = topDocuments.getC_r();
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

    @JsonProperty
    public int getC_sel() {
        return c_sel;
    }

    @JsonProperty
    public int getC_r() {
        return c_r;
    }
}
