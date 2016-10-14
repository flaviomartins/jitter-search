package io.jitter.core.probabilitydistributions;

import jsat.distributions.empirical.KernelDensityEstimator;
import jsat.distributions.empirical.kernelfunc.GaussKF;
import jsat.linear.DenseVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsatKDE implements KDE {

    private static final Logger logger = LoggerFactory.getLogger(JsatKDE.class);

    private double bw;
    private final KernelDensityEstimator kernelDensityEstimator;

    public JsatKDE(double[] data, double[] weights, double bw) {
        this.bw = bw;

        DenseVector dataPoints = new DenseVector(data);
        if (bw <= 0.0) {
            // this.bw = BandwithGuassEstimate(dataPoints); // not normalized (IQR)
            this.bw = KDE.silvermanBandwidthEstimate(data);
        }

        kernelDensityEstimator = new KernelDensityEstimator(dataPoints, GaussKF.getInstance(), this.bw, weights);
    }

    public JsatKDE(double[] data, double[] weights, double bw, METHOD method) {
        this(data, weights, bw);
        if (method == METHOD.REFLECTION) {
            logger.warn("KDE boundary fix is not implemented by JsatKDE. Falling back to STANDARD.");
        }
    }

    @Override
    public double density(double x) {
        return kernelDensityEstimator.pdf(x);
    }

    @Override
    public double getBandwidth() {
        return bw;
    }

}
