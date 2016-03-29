package io.jitter.core.probabilitydistributions;

import jsat.distributions.empirical.KernelDensityEstimator;
import jsat.distributions.empirical.kernelfunc.GaussKF;
import jsat.distributions.empirical.kernelfunc.KernelFunction;
import jsat.linear.DenseVector;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;

public class JsatKDE extends KDE implements ContinuousDistribution {
    private final double[] data;
    private final double[] weights;
    private double bw;
    private KernelDensityEstimator kernelDensityEstimator;
    private METHOD method;

    public JsatKDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.weights = weights;
        this.bw = bw;
        DenseVector dataPoints = new DenseVector(data);
        KernelFunction kf = GaussKF.getInstance();
        kernelDensityEstimator = new KernelDensityEstimator(dataPoints, kf, bw, weights);

        if (bw <= 0.0) {
            this.bw = silvermanBandwidthEstimate(data);
            kernelDensityEstimator.setBandwith(this.bw);
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

        System.out.println(kernelDensityEstimator);
    }

    public JsatKDE(double[] data, double[] weights, double bw, METHOD method) {
        this(data, weights, bw);
        this.method = method;
    }

    public double density(double x) {
        return kernelDensityEstimator.pdf(x);
    }

    public double getBandwidth() {
        return bw;
    }

}
