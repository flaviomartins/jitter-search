package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.probabilitydistributions.JsatKDE;
import io.jitter.core.probabilitydistributions.KDE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KernelDensityReranker extends Reranker {
    private final List<Double> scaledEpochs;
    private KDE kde;
    private double beta = 1.0;

    public KernelDensityReranker(List<Document> results, List<Double> scaledEpochs, double[] trainingData,
                                 double[] trainingWeights) {
        this.results = results;
        this.scaledEpochs = scaledEpochs;
        kde = new JsatKDE(trainingData, trainingWeights, -1.0);

        this.score();
    }

    public KernelDensityReranker(List<Document> results, List<Double> scaledEpochs, double[] trainingData,
                                 double[] trainingWeights, double beta) {
        this.results = results;
        this.scaledEpochs = scaledEpochs;
        this.beta = beta;
        kde = new JsatKDE(trainingData, trainingWeights, -1.0);

        this.score();
    }

    public KernelDensityReranker(List<Document> results, List<Double> scaledEpochs, double[] trainingData,
                                 double[] trainingWeights, KDE.METHOD method, double beta) {
        this.results = results;
        this.scaledEpochs = scaledEpochs;
        this.beta = beta;

        if (trainingData != null && trainingData.length > 2) {
            kde = new JsatKDE(trainingData, trainingWeights, -1.0, method);
        }

        this.score();
    }

    protected void score() {
        Iterator<Document> resultIt = results.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<Document> updatedResults = new ArrayList<>(results.size());
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();
            double scaledEpoch = epochIt.next();
            double density = -100.0;

            if (kde != null) {
                density = kde.density(scaledEpoch);
                density = Math.log(1 + density);
                if (Double.isInfinite(density) || Double.isNaN(density))
                    density = -100.0;
            }

            Document updatedResult = new Document(origResult);
            updatedResult.getFeatures().add((float)density);
            updatedResult.setRsv(origResult.getRsv() + beta * density);
            updatedResults.add(updatedResult);
        }
        results = updatedResults;
    }


}
