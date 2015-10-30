package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.probabilitydistributions.ContinuousDistribution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecencyReranker extends SearchReranker {
    private final ContinuousDistribution distribution;
    private final List<Double> scaledEpochs;

    public RecencyReranker(List<Document> results, ContinuousDistribution distribution,
                           List<Double> scaledEpochs) {
        this.results = results;
        this.scaledEpochs = scaledEpochs;
        this.distribution = distribution;
        this.score();
    }

    protected void score() {
        Iterator<Document> resultIt = results.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<Document> updatedResults = new ArrayList<Document>(results.size());
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();
            double scaledEpoch = epochIt.next();
            double density = distribution.density(scaledEpoch);
            double recency = Math.log(density);
            if (Double.isInfinite(recency) || Double.isNaN(recency))
                recency = -1000.0;
            Document updatedResult = new Document(origResult);
            updatedResult.setRsv(origResult.getRsv() + recency);
            updatedResults.add(updatedResult);
        }
        results = updatedResults;
    }

}
