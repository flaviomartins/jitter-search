package io.jitter.core.probabilitydistributions;

import com.google.common.primitives.Doubles;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;

public class KDE implements ContinuousDistribution {
    public enum METHOD {STANDARD, REFLECTION}

    private final double[] data;
    private final double[] weights;
    private final NormalDistribution kernel;
    private double bw;
    private METHOD method;

    public KDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.bw = bw;
        this.weights = weights;
        kernel = new NormalDistribution(0.0, 1.0);

        if (bw <= 0.0) {
            this.bw = bwSilverman();
        }

        if (weights == null) {
            weights = new double[data.length];
            Arrays.fill(weights, 1.0 / (double) data.length);
        }

        DescriptiveStatistics ds = new DescriptiveStatistics(weights);
        double s = ds.getSum();
        double k = (double) weights.length / s; // factor to make the weights scale correctly

        for (int i = 0; i < weights.length; i++) {
            weights[i] *= k;
        }

    }

    public KDE(double[] data, double[] weights, double bw, METHOD method) {
        this(data, weights, bw);
        this.method = method;
    }

    private double selectSigma() {
        double normalize = 1.349;
        DescriptiveStatistics ds = new DescriptiveStatistics(data);
        double IQR = (ds.getPercentile(75) - ds.getPercentile(25)) / normalize;
        return Math.min(ds.getStandardDeviation(), IQR);
    }

    private double bwSilverman() {
        double A = selectSigma();
        double n = data.length;
        return 0.9 * A * Math.pow(n, -0.2);
    }

    private double bwScott() {
        double A = selectSigma();
        double n = data.length;
        return 1.059 * A * Math.pow(n, -0.2);
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
            f += weights[i] * kernel.density((data[i] - x) / bw);
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
            sum += kernel.density((data[i] - x) / bw);
            sum += kernel.density((data[i] + x - (2 * L)) / bw);
            sum += kernel.density((data[i] + x - (2 * U)) / bw);
            f += weights[i] * sum;
        }
        f /= ((double) weights.length * bw);
        return f;
    }

    public double getBandwidth() {
        return bw;
    }


}
