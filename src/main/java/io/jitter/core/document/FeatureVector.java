package io.jitter.core.document;


import io.jitter.core.utils.*;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;


/**
 * Simple container mapping term->count pairs grabbed from an input text.
 *
 * @author Miles Efron
 */
public class FeatureVector {
    private Map<String, Double> features;
    private double length = 0.0;


    // CONSTRUCTORS
    public FeatureVector(List<String> terms) {
        features = new HashMap<>();
        terms.forEach(this::addTerm);
    }

    public FeatureVector() {
        features = new HashMap<>();
    }


    // MUTATORS

    /**
     * Add all the terms in a string to this vector
     *
     * @param terms a list of string where we want to add each word.
     */
    public void addText(List<String> terms) {
        terms.forEach(this::addTerm);
    }

    /**
     * Add a term to this vector.  if it's already here, increment its count.
     */
    private void addTerm(String term) {
        Double freq = features.get(term);
        if (freq == null) {
            features.put(term, 1.0);
        } else {
            double f = freq;
            features.put(term, f + 1.0);
        }
        length += 1.0;
    }


    /**
     * Add a term to this vector with this weight.  if it's already here, supplement its weight.
     */
    public void addTerm(String term, double weight) {
        Double w = features.get(term);
        if (w == null) {
            features.put(term, weight);
        } else {
            double f = w;
            features.put(term, f + weight);
        }
        length += weight;
    }

    /**
     * in case we want to override the derived length.
     */
    public void setLength(double length) {
        this.length = length;
    }

    public void pruneToSize(int k) {
        List<KeyValuePair> kvpList = getOrderedFeatures();

        Iterator<KeyValuePair> it = kvpList.iterator();

        Map<String, Double> newMap = new HashMap<>(k);
        int i = 0;
        while (it.hasNext()) {
            if (++i > k)
                break;
            KeyValuePair kvp = it.next();
            newMap.put(kvp.getKey(), kvp.getScore());
        }

        features = newMap;

    }

    public void normalizeToOne() {
        Map<String, Double> f = new HashMap<>(features.size());

        double sum = 0;
        for (double value : features.values()) {
            sum += value;
        }

        for (String feature : features.keySet()) {
            double obs = features.get(feature);
            f.put(feature, obs / sum);
        }

        features = f;
    }


    // ACCESSORS

    public Set<String> getFeatures() {
        return features.keySet();
    }

    public double getLength() {
        return length;
    }

    public int getDimensions() {
        return features.size();
    }

    public double getFeatureWeight(String feature) {
        Double w = features.get(feature);
        return (w == null) ? 0.0 : w;
    }

    public Iterator<String> iterator() {
        return features.keySet().iterator();
    }

    public boolean containsKey(String key) {
        return features.containsKey(key);
    }

    public double getVectorNorm() {
        double norm = 0.0;
        for (String s : features.keySet()) {
            norm += Math.pow(features.get(s), 2.0);
        }
        return Math.sqrt(norm);
    }


    // VIEWING

    @Override
    public String toString() {
        return this.toString(features.size());
    }

    public List<KeyValuePair> getOrderedFeatures() {
        List<KeyValuePair> kvpList = new ArrayList<>(features.size());
        for (String feature : features.keySet()) {
            double value = features.get(feature);
            KeyValuePair keyValuePair = new KeyValuePair(feature, value);
            kvpList.add(keyValuePair);
        }
        ScorableComparator comparator = new ScorableComparator(true);
        Collections.sort(kvpList, comparator);

        return kvpList;
    }

    private String toString(int k) {
        NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
        DecimalFormat df = (DecimalFormat)nf;
        df.applyPattern("#.#########");
        StringBuilder b = new StringBuilder();
        List<KeyValuePair> kvpList = getOrderedFeatures();
        Iterator<KeyValuePair> it = kvpList.iterator();
        int i = 0;
        while (it.hasNext() && i++ < k) {
            KeyValuePair pair = it.next();
            b.append(df.format(pair.getScore())).append(" ").append(pair.getKey()).append("\n");
        }
        return b.toString();

    }

    public String buildQuery() {
        return this.buildQuery(features.size());
    }

    String buildQuery(int k) {
        NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
        DecimalFormat df = (DecimalFormat)nf;
        df.applyPattern("#.#########");
        StringBuilder b = new StringBuilder();
        List<KeyValuePair> kvpList = getOrderedFeatures();
        Iterator<KeyValuePair> it = kvpList.iterator();
        int i = 0;
        while (it.hasNext() && i++ < k) {
            KeyValuePair pair = it.next();
            b.append('"').append(pair.getKey()).append('"').append("^").append(df.format(pair.getScore())).append(" ");
        }
        return b.toString();
    }

    // UTILS
    public static FeatureVector interpolate(FeatureVector x, FeatureVector y, double xWeight) {
        FeatureVector z = new FeatureVector();
        Set<String> vocab = new HashSet<>();
        vocab.addAll(x.getFeatures());
        vocab.addAll(y.getFeatures());
        for (String feature : vocab) {
            double weight = xWeight * x.getFeatureWeight(feature) + (1.0 - xWeight) * y.getFeatureWeight(feature);
            z.addTerm(feature, weight);
        }
        return z;
    }

}
