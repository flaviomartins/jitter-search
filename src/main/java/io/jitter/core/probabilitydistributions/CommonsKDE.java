package io.jitter.core.probabilitydistributions;

import com.google.common.primitives.Doubles;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonsKDE implements KDE {

    private static final Logger logger = LoggerFactory.getLogger(CommonsKDE.class);

    private final double[] data;
    private final double[] weights;
    private double bw;
    private final NormalDistribution kf = new NormalDistribution(0.0, 1.0);
    private METHOD method;

    public CommonsKDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.weights = weights;
        this.bw = bw;

        if (bw <= 0.0) {
            this.bw = KDE.scottsBandwidthEstimate(data);
        }

        DescriptiveStatistics ds = new DescriptiveStatistics(weights);
        double sumWeights = ds.getSum();
        double k = (double) weights.length / sumWeights; // factor to make the weights scale correctly
        for (int i = 0; i < weights.length; i++) {
            weights[i] *= k;
        }
    }

    public CommonsKDE(double[] data, double[] weights, double bw, METHOD method) {
        this(data, weights, bw);
        this.method = method;
    }

    @Override
    public double density(double x) {
        if (method == METHOD.REFLECTION) {
            return densityReflection(x);
        }

        return densityStandard(x);
    }

    private double densityStandard(double x) {
        double f = 0.0;
        for (int i = 0; i < data.length; i++) {
            f += weights[i] * kf.density((data[i] - x) / bw);
        }
        f /= ((double) weights.length * bw);
        return f;
    }

    private double densityReflection(double x) {
        double L = Doubles.min(data);
        double U = Doubles.max(data);

        double f = 0.0;
        for (int i = 0; i < data.length; i++) {
            double sum = 0.0;
            sum += kf.density((data[i] - x) / bw);
            sum += kf.density((data[i] + x - (2 * L)) / bw);
            sum += kf.density((data[i] + x - (2 * U)) / bw);
            f += weights[i] * sum;
        }
        f /= ((double) weights.length * bw);
        return f;
    }

    @Override
    public double getBandwidth() {
        return bw;
    }

}
