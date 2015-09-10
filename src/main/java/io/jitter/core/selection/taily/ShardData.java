package io.jitter.core.selection.taily;

public class ShardData {
    double min;
    double df;
    double f;
    double f2;

    public ShardData() {
        min = Double.MAX_VALUE;
        df = 0;
        f = 0;
        f2 = 0;
    }

    public ShardData(double min, double df, double f, double f2) {
        this.min = min;
        this.df = df;
        this.f = f;
        this.f2 = f2;
    }
}
