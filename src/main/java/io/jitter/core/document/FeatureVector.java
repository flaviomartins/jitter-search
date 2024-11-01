package io.jitter.core.document;

import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;

import java.text.DecimalFormat;
import java.util.*;


public class FeatureVector {
    private Object2FloatOpenHashMap<String> features = new Object2FloatOpenHashMap<>();

    public FeatureVector() {}

    public void addFeatureWeight(String term, float weight) {
        if (!features.containsKey(term)) {
            features.put(term, weight);
        } else {
            features.put(term, features.getFloat(term) + weight);
        }
    }

    public FeatureVector pruneToSize(int k) {
        List<KeyValuePair> pairs = getOrderedFeatures();
        Object2FloatOpenHashMap<String> pruned = new Object2FloatOpenHashMap<>();

        int i = 0;
        for (KeyValuePair pair : pairs) {
            if (++i > k)
                break;
            pruned.put(pair.getKey(), pair.getValue());
        }

        this.features = pruned;
        return this;
    }

    public FeatureVector scaleToUnitL2Norm() {
        double norm = computeL2Norm();
        for (String f : features.keySet()) {
            features.put(f, (float) (features.getFloat(f) / norm));
        }

        return this;
    }

    public FeatureVector scaleToUnitL1Norm() {
        double norm = computeL1Norm();
        for (String f : features.keySet()) {
            features.put(f, (float) (features.getFloat(f) / norm));
        }

        return this;
    }

    public Set<String> getFeatures() {
        return features.keySet();
    }

    public float getFeatureWeight(String feature) {
        return features.containsKey(feature) ? features.getFloat(feature) : 0.0f;
    }

    public Iterator<String> iterator() {
        return features.keySet().iterator();
    }

    public boolean contains(String feature) {
        return features.containsKey(feature);
    }

    public double computeL2Norm() {
        double norm = 0.0;
        for (String term : features.keySet()) {
            norm += Math.pow(features.getFloat(term), 2.0);
        }
        return Math.sqrt(norm);
    }

    public double computeL1Norm() {
        double norm = 0.0;
        for (String term : features.keySet()) {
            norm += Math.abs(features.getFloat(term));
        }
        return norm;
    }

    public static FeatureVector fromTerms(List<String> terms) {
        FeatureVector f = new FeatureVector();
        for (String t : terms) {
            f.addFeatureWeight(t, 1.0f);
        }
        return f;
    }

    // VIEWING

    @Override
    public String toString() {
        return this.toString(features.size());
    }

    private List<KeyValuePair> getOrderedFeatures() {
        List<KeyValuePair> kvpList = new ArrayList<>(features.size());
        for (String feature : features.keySet()) {
            float value = features.getFloat(feature);
            KeyValuePair keyValuePair = new KeyValuePair(feature, value);
            kvpList.add(keyValuePair);
        }

        kvpList.sort((x, y) -> {
            double xVal = x.getValue();
            double yVal = y.getValue();

            return (xVal > yVal ? -1 : (xVal == yVal ? 0 : 1));
        });

        return kvpList;
    }

    public Map<String, Float> getMap() {
        List<KeyValuePair> kvpList = getOrderedFeatures();
        LinkedHashMap<String, Float> map = new LinkedHashMap<>(kvpList.size());
        for (KeyValuePair pair : kvpList) {
            map.put(pair.getKey(), pair.getValue());
        }
        return map;
    }

    public String toString(int k) {
        DecimalFormat format = new DecimalFormat("#.#########");
        StringBuilder b = new StringBuilder();
        List<KeyValuePair> kvpList = getOrderedFeatures();
        Iterator<KeyValuePair> it = kvpList.iterator();
        int i = 0;
        while (it.hasNext() && i++ < k) {
            KeyValuePair pair = it.next();
            b.append(format.format(pair.getValue())).append(" ").append(pair.getKey()).append("\n");
        }
        return b.toString();

    }

    public static FeatureVector interpolate(FeatureVector x, FeatureVector y, float xWeight) {
        FeatureVector z = new FeatureVector();
        Set<String> vocab = new HashSet<>();
        vocab.addAll(x.getFeatures());
        vocab.addAll(y.getFeatures());
        for (String feature : vocab) {
            float weight = (float) (xWeight * x.getFeatureWeight(feature) + (1.0 - xWeight)
                    * y.getFeatureWeight(feature));
            z.addFeatureWeight(feature, weight);
        }
        return z;
    }

    public static FeatureVector add(FeatureVector x, FeatureVector y) {
        FeatureVector z = new FeatureVector();
        Set<String> vocab = new HashSet<>();
        vocab.addAll(x.getFeatures());
        vocab.addAll(y.getFeatures());
        for (String feature : vocab) {
            float weight = x.getFeatureWeight(feature) + y.getFeatureWeight(feature);
            z.addFeatureWeight(feature, weight);
        }
        return z;
    }

    private static class KeyValuePair {
        private final String key;
        private final float value;

        public KeyValuePair(String key, float value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        @Override
        public String toString() {
            return value + "\t" + key;
        }

        public float getValue() {
            return value;
        }
    }
}
