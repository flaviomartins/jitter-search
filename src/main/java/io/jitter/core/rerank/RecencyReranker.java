package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.probabilitydistributions.ContinuousDistribution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecencyReranker implements Reranker {
    private final ContinuousDistribution distribution;
    private final List<Double> scaledEpochs;

    public RecencyReranker(ContinuousDistribution distribution,
                           List<Double> scaledEpochs) {
        this.scaledEpochs = scaledEpochs;
        this.distribution = distribution;
    }

    @Override
    public List<Document> rerank(List<Document> docs, RerankerContext context) {
        Iterator<Document> resultIt = docs.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<Document> updatedResults = new ArrayList<>(docs.size());
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();
            double scaledEpoch = epochIt.next();
            double density = distribution.density(scaledEpoch);
            double recency = Math.log(density);
            if (Double.isInfinite(recency) || Double.isNaN(recency))
                recency = -1000.0;
            Document updatedResult = new Document(origResult);
            updatedResult.getFeatures().add((float)density);
            updatedResult.setRsv(origResult.getRsv() + recency);
            updatedResults.add(updatedResult);
        }
        return updatedResults;
    }

}
