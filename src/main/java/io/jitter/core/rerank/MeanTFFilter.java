package io.jitter.core.rerank;

import com.google.common.math.Stats;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.document.DocVector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MeanTFFilter implements Reranker {
    private final int maxMeanTF;

    public MeanTFFilter(int maxMeanTF) {
        this.maxMeanTF = maxMeanTF;
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
                double meanFreq = Stats.meanOf(freqs);
                if (meanFreq >= maxMeanTF) {
                    continue;
                }
            }

            updatedResults.add(origResult);
        }
        return updatedResults;
    }

}
