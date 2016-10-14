package io.jitter.core.filter;

import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;

import java.util.*;

public class MaxTFFilter extends Filter {
    private final int maxTF;

    public MaxTFFilter(int maxTF) {
        this.maxTF = maxTF;
    }

    public void setResults(List<Document> results) {
        this.results = results;
        this.filter();
    }

    @Override
    protected void filter() {
        Iterator<Document> resultIt = results.iterator();

        List<Document> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            DocVector docVector = origResult.getDocVector();
            if (docVector != null) {
                Collection<Integer> freqs = docVector.vector.values();
                Integer maxFreq = Collections.max(freqs);
                if (maxFreq >= maxTF) {
                    continue;
                }
            }

            updatedResults.add(origResult);
        }
        results = updatedResults;
    }

}
