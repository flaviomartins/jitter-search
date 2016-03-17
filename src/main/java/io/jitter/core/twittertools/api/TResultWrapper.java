package io.jitter.core.twittertools.api;

import cc.twittertools.thrift.gen.TResult;
import ciir.umass.edu.learning.DataPoint;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true, value = {"setId", "setRsv", "setScreen_name", "setEpoch", "setText",
        "setFollowers_count", "setStatuses_count", "setLang", "setIn_reply_to_status_id", "setIn_reply_to_user_id",
        "setRetweeted_status_id", "setRetweeted_user_id", "setRetweeted_count", "features", "properties", "dataPoint"})
public class TResultWrapper extends TResult {
    private ArrayList<Float> features;

    public TResultWrapper() {
        super();
        features = new ArrayList<>();
    }

    public TResultWrapper(TResult other) {
        super(other);
        features = new ArrayList<>();
    }

    public TResultWrapper(TResultWrapper other) {
        super(other);
        this.features = other.features;
    }

    public ArrayList<Float> getFeatures() {
        return features;
    }

    public void setFeatures(ArrayList<Float> features) {
        this.features = features;
    }

    public DataPoint getDataPoint() {
        return new TResultDataPoint(Long.toString(id), features);
    }
}
