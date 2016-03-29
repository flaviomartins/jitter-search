package io.jitter.core.probabilitydistributions;

import com.google.common.primitives.Doubles;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class CommonsKDE implements KDE  {
    private final double[] data;
    private final double[] weights;
    private double bw;
    private final NormalDistribution kf = new NormalDistribution(0.0, 1.0);;
    private METHOD method;

    public CommonsKDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.weights = weights;
        this.bw = bw;

        if (bw <= 0.0) {
            this.bw = silvermanBandwidthEstimate(data);
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

    public double density(double x) {
        if (METHOD.REFLECTION.equals(method)) {
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

    public double getBandwidth() {
        return bw;
    }

    private static double selectSigma(double[] X) {
        double normalize = 1.349;
        DescriptiveStatistics ds = new DescriptiveStatistics(X);
        double IQR = (ds.getPercentile(75) - ds.getPercentile(25)) / normalize;
        return Math.min(ds.getStandardDeviation(), IQR);
    }

    public static double silvermanBandwidthEstimate(double[] X) {
        double A = selectSigma(X);

        if (X.length == 1)
            return 1;
        else if (A == 0)
            return 1.06 * Math.pow(X.length, -1.0/5.0);
        return 1.06 * A * Math.pow(X.length, -1.0/5.0);
    }

}
