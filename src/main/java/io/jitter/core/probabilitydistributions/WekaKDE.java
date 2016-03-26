package io.jitter.core.probabilitydistributions;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import weka.estimators.KernelEstimator;

import java.util.Arrays;

public class WekaKDE implements KDE {
    private final double[] data;
    private final double[] weights;
    private double bw;
    private KernelEstimator kernelEstimator;
    private METHOD method;

    public WekaKDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.weights = weights;
        this.bw = bw;
        kernelEstimator = new KernelEstimator(0);

        if (bw <= 0.0) {
            this.bw = bwSilverman();
        }

        if (weights == null) {
            weights = new double[data.length];
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

        for (int i = 0; i < data.length; i++) {
            kernelEstimator.addValue(data[i], weights[i]);
        }
        System.out.println(kernelEstimator);
    }

    public WekaKDE(double[] data, double[] weights, double bw, METHOD method) {
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
        return kernelEstimator.getProbability(x);
    }

    public double getBandwidth() {
        return bw;
    }

}
