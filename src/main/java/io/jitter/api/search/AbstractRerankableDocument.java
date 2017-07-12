package io.jitter.api.search;

import ciir.umass.edu.learning.DataPoint;
import io.jitter.core.twittertools.api.DocumentDataPoint;

import java.util.ArrayList;

public abstract class AbstractRerankableDocument extends AbstractDocument implements Document {
    private ArrayList<Float> features = new ArrayList<>();

    public ArrayList<Float> getFeatures() {
        return features;
    }

    public void setFeatures(ArrayList<Float> features) {
        this.features = features;
    }

    public DataPoint getDataPoint() {
        return new DocumentDataPoint(getId(), features);
    }
}
