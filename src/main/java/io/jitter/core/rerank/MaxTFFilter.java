package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;

import java.util.*;

public class MaxTFFilter implements Reranker {
    private final int maxTF;

    public MaxTFFilter(int maxTF) {
        this.maxTF = maxTF;
    }

    @Override
    public List<Document> rerank(List<Document> docs, RerankerContext context) {
        Iterator<Document> resultIt = docs.iterator();

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
        return updatedResults;
    }

}
