package io.jitter.core.feedback;

import io.jitter.api.search.Document;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.utils.KeyValuePair;
import io.jitter.core.utils.ScorableComparator;
import io.jitter.core.utils.Stopper;

import java.text.DecimalFormat;
import java.util.*;


public abstract class FeedbackModel {
    protected List<Document> relDocs;
    protected FeatureVector originalQueryFV;
    protected List<KeyValuePair> features;        // these will be KeyValuePair objects
    protected Stopper stopper;


    public void build(Stopper stopper) {
        this.stopper = stopper;
    }


    public FeatureVector asFeatureVector() {
        FeatureVector f = new FeatureVector(stopper);

        for (KeyValuePair tuple : features) {
            f.addTerm(tuple.getKey(), tuple.getScore());
        }

        return f;
    }

    public Map<String, Double> asMap() {
        Map<String, Double> map = new HashMap<>(features.size());
        for (KeyValuePair tuple : features) {
            map.put(tuple.getKey(), tuple.getScore());
        }

        return map;
    }

    @Override
    public String toString() {
        return toString(features.size());
    }

    public String toString(int k) {
        DecimalFormat format = new DecimalFormat("#.#####################");


        ScorableComparator comparator = new ScorableComparator(true);
        Collections.sort(features, comparator);

        double sum = 0.0;
        Iterator<KeyValuePair> it = features.iterator();
        int i = 0;
        while (it.hasNext() && i++ < k) {
            sum += it.next().getScore();
        }

        StringBuilder b = new StringBuilder();
        it = features.iterator();
        i = 0;
        while (it.hasNext() && i++ < k) {
            KeyValuePair tuple = it.next();
            b.append(format.format(tuple.getScore() / sum)).append(" ").append(tuple.getKey()).append("\n");
        }

        return b.toString();
    }


    public void setRes(List<Document> relDocs) {
        this.relDocs = relDocs;
    }

    public void setOriginalQueryFV(FeatureVector originalQueryFV) {
        this.originalQueryFV = originalQueryFV;
    }

}
