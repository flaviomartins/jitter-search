package io.jitter.api.search;

import ciir.umass.edu.learning.DataPoint;
import io.jitter.core.document.DocVector;
import io.jitter.core.twittertools.api.DocumentDataPoint;

import java.util.ArrayList;

public abstract class AbstractDocument {
    private int shardId;
    private ArrayList<Float> features = new ArrayList<>();

    public abstract String getId();

    public abstract void setId(String id);

    public abstract double getRsv();

    public abstract void setRsv(double score);

    public abstract String getText();

    public abstract void setText(String text);

    public abstract DocVector getDocVector();

    public abstract void setDocVector(DocVector newDocVector);

    public ArrayList<Float> getFeatures() {
        return features;
    }

    public void setFeatures(ArrayList<Float> features) {
        this.features = features;
    }

    public DataPoint getDataPoint() {
        return new DocumentDataPoint(getId(), features);
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }
}
