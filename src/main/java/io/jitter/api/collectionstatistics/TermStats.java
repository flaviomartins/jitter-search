package io.jitter.api.collectionstatistics;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TermStats {

    private String term;
    private int docFreq;
    private long termFreq;

    public TermStats() {
        // Jackson deserialization
    }

    public TermStats(String term, int docFreq, long totalTermFreq) {
        this.term = term;
        this.docFreq = docFreq;
        this.termFreq = totalTermFreq;
    }

    @JsonProperty
    public String getTerm() {
        return term;
    }

    @JsonProperty
    public int getDocFreq() {
        return docFreq;
    }

    @JsonProperty
    public long getTermFreq() {
        return termFreq;
    }
}
