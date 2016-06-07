package io.jitter.core.features;

public class IndriFeature {
    public static final float DEFAULT_MU = 2500;

    private float mu;

    public IndriFeature() {
        this(DEFAULT_MU);
    }

    public IndriFeature(float mu) {
        this.mu = mu;
    }

    public float getMu() {
        return mu;
    }

    public void setMu(float mu) {
        this.mu = mu;
    }

    public double value(double tf, double ctf, double totalTermCount, double docLength) {
        return Math.log((tf + mu * (ctf / totalTermCount)) / (docLength + mu));
    }

}
