package io.jitter.api.collectionstatistics;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TermStats {

    private String termText;
    private int docFreq;
    private long termFreq;

    public TermStats() {
        // Jackson deserialization
    }

    public TermStats(String termText, int docFreq, long totalTermFreq) {
        this.termText = termText;
        this.docFreq = docFreq;
        this.termFreq = totalTermFreq;
    }

    @JsonProperty
    public String getTermText() {
        return termText;
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
