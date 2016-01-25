package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"method", "c_sel", "c_r", "sources", "topics", "numFound", "start", "selectDocs", "shardDocs", "docs"})
public class RMTSDocumentsResponse extends SelectionSearchDocumentsResponse {
    
    private List<?> docs;

    public RMTSDocumentsResponse() {
        // Jackson deserialization
    }

    public RMTSDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int numFound, int start, List<?> selectDocs, List<?> docs) {
        super(sources, topics, method, numFound, start, selectDocs, null);
        this.docs = docs;
    }

    public RMTSDocumentsResponse(Map<String, Double> sources, Map<String, Double> topics, String method, int start, SelectionTopDocuments selectionTopDocuments, TopDocuments topDocuments) {
        super(sources, topics, method, start, selectionTopDocuments, null);
        this.numFound = topDocuments.totalHits;
        this.docs = topDocuments.scoreDocs;
    }

    @JsonProperty
    public List<?> getDocs() {
        return docs;
    }
}
