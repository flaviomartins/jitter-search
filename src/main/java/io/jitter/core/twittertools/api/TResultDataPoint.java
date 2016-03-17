package io.jitter.core.twittertools.api;

import ciir.umass.edu.learning.DenseDataPoint;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;

public class TResultDataPoint extends DenseDataPoint {

    public TResultDataPoint(String description, ArrayList<Float> features) {
        super("0 qid:1");
        setDescription(description);
        setFeatureVector(ArrayUtils.toPrimitive(features.toArray(new Float[0])));
    }
}
