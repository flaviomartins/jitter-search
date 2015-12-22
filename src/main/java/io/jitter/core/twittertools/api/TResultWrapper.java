package io.jitter.core.twittertools.api;

import cc.twittertools.thrift.gen.TResult;
import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.DenseDataPoint;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true, value = {"setId", "setRsv", "setScreen_name", "setEpoch", "setText",
        "setFollowers_count", "setStatuses_count", "setLang", "setIn_reply_to_status_id", "setIn_reply_to_user_id",
        "setRetweeted_status_id", "setRetweeted_user_id", "setRetweeted_count", "features", "properties", "dataPoint"})
public class TResultWrapper extends TResult {
    private ArrayList<Float> features;
    private Map<String, Float> properties;

    public TResultWrapper() {
        super();
        features = new ArrayList<>();
        properties = new HashMap<>();
    }

    public TResultWrapper(TResult other) {
        super(other);
        features = new ArrayList<>();
        properties = new HashMap<>();
    }

    public TResultWrapper(TResultWrapper other) {
        super(other);
        this.features = other.features;
        this.properties = other.properties;
    }

    public ArrayList<Float> getFeatures() {
        return features;
    }

    public void setFeatures(ArrayList<Float> features) {
        this.features = features;
    }

    public Map<String, Float> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Float> properties) {
        this.properties = properties;
    }

    public DataPoint getDataPoint(String rel, String qid) {
        StringBuilder sb = new StringBuilder();
        sb.append(rel).append(" ");
        sb.append("qid:").append(qid).append(" ");
        int i = 1;
        for (Float fVal : features) {
            sb.append(String.format(Locale.ROOT, "%d:%f", i, fVal)).append(" ");
            i++;
        }
        sb.append("# ").append(id);
        String text = sb.toString().trim();
        return new DenseDataPoint(text);
    }

    public DataPoint getDataPoint() {
        StringBuilder sb = new StringBuilder();
        sb.append(0).append(" ");
        sb.append("qid:").append(1).append(" ");
        int i = 1;
        for (Float fVal : features) {
            sb.append(String.format(Locale.ROOT, "%d:%f", i, fVal)).append(" ");
            i++;
        }
        sb.append("# ").append(id);
        String text = sb.toString().trim();
        return new DenseDataPoint(text);
    }
}
