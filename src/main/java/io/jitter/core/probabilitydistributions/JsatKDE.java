package io.jitter.core.probabilitydistributions;

import jsat.distributions.empirical.KernelDensityEstimator;
import jsat.distributions.empirical.kernelfunc.GaussKF;
import jsat.distributions.empirical.kernelfunc.KernelFunction;
import jsat.linear.DenseVector;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import static jsat.distributions.empirical.KernelDensityEstimator.BandwithGuassEstimate;

public class JsatKDE implements KDE {
    private final double[] data;
    private final double[] weights;
    private double bw;
    private final KernelFunction kf = GaussKF.getInstance();
    private final KernelDensityEstimator kernelDensityEstimator;
    private METHOD method;

    public JsatKDE(double[] data, double[] weights, double bw) {
        this.data = data;
        this.weights = weights;
        this.bw = bw;

        DenseVector dataPoints = new DenseVector(data);
        if (bw <= 0.0) {
            // this.bw = BandwithGuassEstimate(dataPoints); // not normalized (IQR)
            this.bw = silvermanBandwidthEstimate(data);
        }

        kernelDensityEstimator = new KernelDensityEstimator(dataPoints, kf, this.bw, weights);
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
