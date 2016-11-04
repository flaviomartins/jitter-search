package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.probabilitydistributions.JsatKDE;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.utils.TimeUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KernelDensityReranker implements Reranker {
    public static final double DAY = 60.0 * 60.0 * 24.0;

    private final double[] trainingData;
    private final double[] trainingWeights;
    private final KDE.METHOD method;
    private double beta = 1.0;

    public KernelDensityReranker(double[] trainingData, double[] trainingWeights, KDE.METHOD method, double beta) {
        this.trainingData = trainingData;
        this.trainingWeights = trainingWeights;
        this.method = method;
        this.beta = beta;
    }

    @Override
    public List<Document> rerank(List<Document> docs, RerankerContext context) {
        double queryEpoch = context.getQueryEpoch();
        // extract raw epochs from results
        List<Double> rawEpochs = TimeUtils.extractEpochsFromResults(docs);
        // groom our hit times wrt to query time
        List<Double> scaledEpochs = TimeUtils.adjustEpochsToLandmark(rawEpochs, queryEpoch, DAY);

        KDE kde = new JsatKDE(trainingData, trainingWeights, -1.0, method);

        Iterator<Document> resultIt = docs.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<Document> updatedResults = new ArrayList<>(docs.size());
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();
            double scaledEpoch = epochIt.next();
            double density = 0;

            if (kde != null) {
                density = kde.density(scaledEpoch);
                if (Double.isInfinite(density) || Double.isNaN(density))
                    density = 0;
            }

            Document updatedResult = new Document(origResult);
            updatedResult.getFeatures().add((float)density);
            updatedResult.setRsv(origResult.getRsv() + beta * density);
            updatedResults.add(updatedResult);
        }
        return updatedResults;
    }


}
