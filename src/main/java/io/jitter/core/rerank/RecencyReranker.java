package io.jitter.core.rerank;

import io.jitter.api.search.StatusDocument;
import io.jitter.core.probabilitydistributions.LocalExponentialDistribution;
import io.jitter.core.utils.TimeUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecencyReranker implements Reranker {
    public static final double DAY = 60.0 * 60.0 * 24.0;

    private final double lambda;

    public RecencyReranker(double lambda) {
        this.lambda = lambda;
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        double queryEpoch = context.getQueryEpoch();
        // extract raw epochs from results
        List<Double> rawEpochs = TimeUtils.extractEpochsFromResults(docs);
        // groom our hit times wrt to query time
        List<Double> scaledEpochs = TimeUtils.adjustEpochsToLandmark(rawEpochs, queryEpoch, DAY);

        LocalExponentialDistribution distribution = new LocalExponentialDistribution(lambda);

        Iterator<StatusDocument> resultIt = docs.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<StatusDocument> updatedResults = new ArrayList<>(docs.size());
        while (resultIt.hasNext()) {
            StatusDocument origResult = resultIt.next();
            double scaledEpoch = epochIt.next();
            double density = distribution.density(scaledEpoch);
            double recency = Math.log(density);
            if (Double.isInfinite(recency) || Double.isNaN(recency))
                recency = -1000.0;
            StatusDocument updatedResult = new StatusDocument(origResult);
            updatedResult.getFeatures().add((float)density);
            //TODO: why set Rsv here?
            updatedResult.setRsv(origResult.getRsv() + recency);
            updatedResults.add(updatedResult);
        }
        return updatedResults;
    }

}
