package io.jitter.core.rerank;

import io.jitter.api.search.StatusDocument;
import io.jitter.core.document.DocVector;

import java.util.*;

public class MaxTFFilter implements Reranker {
    private final int maxTF;

    public MaxTFFilter(int maxTF) {
        this.maxTF = maxTF;
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        Iterator<StatusDocument> resultIt = docs.iterator();

        List<StatusDocument> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            StatusDocument origResult = resultIt.next();

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
        return updatedResults;
    }

}
