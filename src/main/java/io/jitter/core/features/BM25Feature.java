package io.jitter.core.features;

public class BM25Feature {
    public static final double DEFAULT_K_1 = 1.2;
    public static final double DEFAULT_B = 0.75;

    private double k_1;
    private double b;

    public BM25Feature() {
        this(DEFAULT_K_1, DEFAULT_B);
    }

    public BM25Feature(double k_1, double b) {
        this.k_1 = k_1;
        this.b = b;
    }

    public double getK_1() {
        return k_1;
    }

    public void setK_1(double k_1) {
        this.k_1 = k_1;
    }

    public double getB() {
        return b;
    }

    public void setB(double b) {
        this.b = b;
    }

    public double value(double tf, double docLength, double averageDocumentLength,
                        double n_t, double numberOfDocuments) {
        double K = k_1 * ((1 - b) + b * docLength / averageDocumentLength) + tf;
        return Math.log((numberOfDocuments - n_t + 0.5d) / (n_t + 0.5d)) *
                ((k_1 + 1d) * tf / (K + tf));
    }

}
