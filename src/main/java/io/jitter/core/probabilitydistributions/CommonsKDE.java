package io.jitter.core.probabilitydistributions;

import com.google.common.primitives.Doubles;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;

public class CommonsKDE extends KDE implements ContinuousDistribution {
    private final double[] data;
    private final double[] weights;
    private double bw;
    private final NormalDistribution kernel;
    private METHOD method;

    public CommonsKDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.weights = weights;
        this.bw = bw;
        kernel = new NormalDistribution(0.0, 1.0);

        if (bw <= 0.0) {
            this.bw = silvermanBandwidthEstimate(data);
        }

        DescriptiveStatistics ds = new DescriptiveStatistics(weights);
        double sum = ds.getSum();

        if (sum == 0) {
            Arrays.fill(weights, 1.0 / (double) data.length);
        }

        ds = new DescriptiveStatistics(weights);
        sum = ds.getSum();

        double k = (double) weights.length / sum; // factor to make the weights scale correctly
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
