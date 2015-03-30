package org.novasearch.jitter.core.selection.taily;

public class IndriFeature {
    public static final int DEFAULT_MU = 2500;

    private int mu;

    public IndriFeature() {
        this(DEFAULT_MU);
    }

    public IndriFeature(int mu) {
        this.mu = mu;
    }

    public int getMu() {
        return mu;
    }

    public void setMu(int mu) {
        this.mu = mu;
    }

    public double value(double tf, double ctf, double totalTermCount, double docLength) {
        return Math.log((tf + mu * (ctf / totalTermCount)) / (docLength + mu));
    }

}
