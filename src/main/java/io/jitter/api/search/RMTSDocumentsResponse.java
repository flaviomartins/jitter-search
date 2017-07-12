package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;

import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonPropertyOrder({"method", "c_sel", "c_r", "sources", "topics", "numFound", "start", "selectDocs", "shardDocs", "docs"})
public class RMTSDocumentsResponse extends SelectionSearchDocumentsResponse {
    
    private List<?> docs;

    public RMTSDocumentsResponse() {
        // Jackson deserialization
    }

    public RMTSDocumentsResponse(Set<Map.Entry<String, Double>> sources, Set<Map.Entry<String, Double>> topics, String method, int numFound, int start, List<?> selectDocs, List<?> shardDocs, List<?> docs) {
        super(sources, topics, method, numFound, start, selectDocs, shardDocs);
        this.docs = docs;
    }

    public RMTSDocumentsResponse(Set<Map.Entry<String, Double>> sources, Set<Map.Entry<String, Double>> topics, String method, int start, SelectionTopDocuments selectionTopDocuments, SelectionTopDocuments shardTopDocuments, TopDocuments topDocuments) {
        super(sources, topics, method, start, selectionTopDocuments, shardTopDocuments);
        this.numFound = topDocuments.totalHits;
        this.docs = topDocuments.scoreDocs;
    }

    @JsonProperty
    public List<?> getDocs() {
        return docs;
    }
}
