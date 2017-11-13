package io.jitter.core.rerank;

import com.google.common.primitives.Doubles;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.probabilitydistributions.CommonsKDE;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.utils.TimeUtils;
import org.apache.commons.math3.util.FastMath;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class KDEReranker implements Reranker {
    public static final double DAY = 60.0 * 60.0 * 24.0;

    public enum WEIGHT {UNIFORM, SCORE, RANK}

    private final KDE.METHOD method;
    private final WEIGHT scheme;
    private double beta = 1.0;

    public KDEReranker(KDE.METHOD method, WEIGHT scheme, double beta) {
        this.method = method;
        this.scheme = scheme;
        this.beta = beta;
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        double queryEpoch = context.getQueryEpoch();
        // extract raw epochs from results
        List<Double> rawEpochs = TimeUtils.extractEpochsFromResults(docs);
        // groom our hit times wrt to query time
        List<Double> scaledEpochs = TimeUtils.adjustEpochsToLandmark(rawEpochs, queryEpoch, DAY);

        double[] densityTrainingData = Doubles.toArray(scaledEpochs);
        double[] densityWeights = new double[densityTrainingData.length];

        switch (scheme) {
            case SCORE:
                Iterator<StatusDocument> resultIt = docs.iterator();
                int j = 0;
                double maxRsv = Double.NEGATIVE_INFINITY;
                while (resultIt.hasNext()) {
                    StatusDocument result = resultIt.next();
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
                double mean = 0;
                for (int i = 1; i <= densityWeights.length; i++) {
                    mean += i * 1.0 / densityWeights.length;
                }
                double lambda = 1.0 / mean;
                for (int i = 1; i <= densityWeights.length; i++) {
                    densityWeights[i - 1] = lambda * FastMath.exp(-i);
                }
                break;
            case UNIFORM:
            default:
                Arrays.fill(densityWeights, 1.0 / densityWeights.length);
        }

        KDE kde = new CommonsKDE(densityTrainingData, densityWeights, -1.0, method);

        Iterator<StatusDocument> resultIt = docs.iterator();
        Iterator<Double> epochIt = scaledEpochs.iterator();

        List<StatusDocument> updatedResults = new ArrayList<>(docs.size());
        while (resultIt.hasNext()) {
            StatusDocument origResult = resultIt.next();
            double scaledEpoch = epochIt.next();
            double density = 0;

            if (kde != null) {
                density = kde.density(scaledEpoch);
                if (Double.isInfinite(density) || Double.isNaN(density))
                    density = 0;
            }

            StatusDocument updatedResult = new StatusDocument(origResult);
            updatedResult.getFeatures().add((float)density);
            updatedResult.setRsv(origResult.getRsv() + beta * density);
            updatedResults.add(updatedResult);
        }
        return updatedResults;
    }

}
