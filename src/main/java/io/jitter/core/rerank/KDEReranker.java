package io.jitter.core.rerank;

import com.google.common.primitives.Doubles;
import io.jitter.api.search.Document;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.twittertools.api.TResultWrapper;
import io.jitter.core.utils.TimeUtils;
import org.apache.commons.math3.util.FastMath;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class KDEReranker extends SearchReranker {
    public static final double DAY = 60.0 * 60.0 * 24.0;
    public static final double HOUR = 60.0 * 60.0;

    public enum WEIGHT {UNIFORM, SCORE, RANK}

    private final List<Double> scaledEpochs;
    private final KDE kde;
    private double beta = 1.0;

    public KDEReranker(List<Document> results, double queryEpoch, KDE.METHOD method, WEIGHT scheme, double beta) {
        this.results = results;
        this.beta = beta;

        List<Double> rawEpochs = TimeUtils.extractEpochsFromResults(results);
        // groom our hit times wrt to query time
        scaledEpochs = TimeUtils.adjustEpochsToLandmark(rawEpochs, queryEpoch, DAY);

        double[] densityTrainingData = Doubles.toArray(scaledEpochs);
        double[] densityWeights = new double[densityTrainingData.length];
        kde = new KDE(densityTrainingData, densityWeights, -1.0, method);

        switch (scheme) {
            case SCORE:
                Iterator<Document> resultIt = results.iterator();
                int j = 0;
                double maxRsv = Double.NEGATIVE_INFINITY;
                while (resultIt.hasNext()) {
                    TResultWrapper result = resultIt.next();
                    double rsv = result.getRsv();
                    // deal with munged lucene QL's
                    if (rsv < 0.0)
                        rsv = FastMath.exp(rsv);
                    densityWeights[j++] = rsv;
                    if (rsv > maxRsv)
                        maxRsv = rsv;
                }
                break;
            case RANK:
                Iterator<Document> resultIt1 = results.iterator();
                int jj = 0;
//                (n/2)(n+1)
//                double lambda = 1.0 / (results.size()/2.0)*(results.size()+1);
                double lambda = 1.0 / (results.size()/2.0);
                while (resultIt1.hasNext()) {
                    TResultWrapper result = resultIt1.next();
//                    double weight = lambda * Math.exp(-1.0 * lambda * (j+1));
//                    double weight = 1.0 / Math.pow(j+1, 2);
                    double weight = 1.0 / (jj+1 + 60);
                    densityWeights[jj++] = weight;
                }
                break;
            case UNIFORM:
            default:
                Arrays.fill(densityWeights, 1.0 / (double) densityWeights.length);
        }

        this.score();
    }

    protected void score() {
        Iterator<Document> resultIt = results.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<Document> updatedResults = new ArrayList<>(results.size());
        while (resultIt.hasNext()) {
            TResultWrapper origResult = resultIt.next();
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

    public double getBandwidth() {
        return kde.getBandwidth();
    }

}
