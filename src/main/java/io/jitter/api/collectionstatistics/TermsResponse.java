package io.jitter.api.collectionstatistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.util.List;

public class TermsResponse {

    private int numFound;
    private int start;
    private String shardId;
    private List<TermStats> terms;

    public TermsResponse() {
        // Jackson deserialization
    }

    public TermsResponse(int numFound, int start, String shardId, org.apache.lucene.misc.TermStats[] termStatsArray) {
        this.numFound = numFound;
        this.start = start;
        this.shardId = shardId;
        terms = Lists.newArrayList();
        for (org.apache.lucene.misc.TermStats t : termStatsArray) {
            terms.add(new TermStats(t.termtext.utf8ToString(), t.docFreq, t.totalTermFreq));
        }
    }

    public TermsResponse(int totalHits, int i, org.apache.lucene.misc.TermStats[] terms) {
        this(totalHits, i, "-1", terms);
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
    public String getShardId() {
        return shardId;
    }

    @JsonProperty
    public List<TermStats> getTerms() {
        return terms;
    }
}
