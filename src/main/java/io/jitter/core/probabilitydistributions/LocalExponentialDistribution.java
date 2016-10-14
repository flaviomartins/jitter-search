package io.jitter.core.probabilitydistributions;

import org.apache.commons.math3.util.FastMath;

public class LocalExponentialDistribution implements ContinuousDistribution {
    private double lambda = 0.01;

    public LocalExponentialDistribution(double lambda) {
        this.lambda = lambda;
    }

    @Override
    public double density(double x) {
        return lambda * FastMath.exp(-1.0 * lambda * x);
    }

}
