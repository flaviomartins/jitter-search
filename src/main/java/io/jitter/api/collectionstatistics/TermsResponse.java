package io.jitter.api.collectionstatistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.util.List;

public class TermsResponse {

    private int numFound;
    private int start;
    private List<TermStats> terms;

    public TermsResponse() {
        // Jackson deserialization
    }

    public TermsResponse(int numFound, int start, org.apache.lucene.misc.TermStats[] termStatsArray) {
        this.numFound = numFound;
        this.start = start;
        terms = Lists.newArrayList();
        for (org.apache.lucene.misc.TermStats t : termStatsArray) {
            terms.add(new TermStats(t.termtext.utf8ToString(), t.docFreq, t.totalTermFreq));
        }
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
    public List<TermStats> getTerms() {
        return terms;
    }
}
