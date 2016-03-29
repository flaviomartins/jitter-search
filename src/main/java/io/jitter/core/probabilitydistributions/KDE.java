package io.jitter.core.probabilitydistributions;

public interface KDE extends ContinuousDistribution {

    enum METHOD {STANDARD, REFLECTION}

    double density(double x);

    double getBandwidth();

}
