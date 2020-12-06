package io.jitter.core.twittertools.api;

import ciir.umass.edu.learning.DenseDataPoint;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;

public class DocumentDataPoint extends DenseDataPoint {

    public DocumentDataPoint(String description, ArrayList<Float> features) {
        super("0 qid:1");
        setDescription(description);
        fVals = new float[features.size() + 1];
        float[] dfVals = ArrayUtils.toPrimitive(features.toArray(new Float[features.size()]));
        System.arraycopy(dfVals, 0, fVals, 1, features.size());
    }
}
