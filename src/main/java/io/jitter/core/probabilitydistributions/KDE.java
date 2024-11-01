package io.jitter.core.probabilitydistributions;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public interface KDE extends ContinuousDistribution {

    enum METHOD {STANDARD, REFLECTION}

    @Override
    double density(double x);

    double getBandwidth();

    static double selectSigma(double[] X) {
        double normalize = 1.349;
        DescriptiveStatistics ds = new DescriptiveStatistics(X);
        double IQR = (ds.getPercentile(75) - ds.getPercentile(25)) / normalize;
        return Math.min(ds.getStandardDeviation(), IQR);
    }

    static double scottsBandwidthEstimate(double[] X) {
        double A = selectSigma(X);

        if (X.length == 1)
            return 1;
        else if (A == 0)
            return 1.06 * Math.pow(X.length, -1.0/5.0);
        return 1.06 * A * Math.pow(X.length, -1.0/5.0);
    }


    static double silvermanBandwidthEstimate(double[] X) {
        double A = selectSigma(X);

        if (X.length == 1)
            return 1;
        else if (A == 0)
            return 0.9 * Math.pow(X.length, -1.0/5.0);
        return 0.9 * A * Math.pow(X.length, -1.0/5.0);
    }
}
