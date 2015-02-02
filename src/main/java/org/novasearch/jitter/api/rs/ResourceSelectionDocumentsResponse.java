package org.novasearch.jitter.api.rs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.novasearch.jitter.api.search.Document;

import java.util.List;
import java.util.Map;

public class ResourceSelectionDocumentsResponse {

    private Map<String, Integer> sources;
    private int numFound;
    private int start;
    private List<Document> docs;

    public ResourceSelectionDocumentsResponse() {
        // Jackson deserialization
    }

    public ResourceSelectionDocumentsResponse(int numFound, int start, Map<String, Integer> sources, List<Document> docs) {
        this.numFound = numFound;
        this.start = start;
        this.sources = sources;
        this.docs = docs;
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
    public Map<String, Integer> getSources() {
        return sources;
    }

    @JsonProperty
    public List<Document> getDocs() {
        return docs;
    }
}
