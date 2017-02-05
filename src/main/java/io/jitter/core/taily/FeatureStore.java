package io.jitter.core.taily;

public interface FeatureStore {
    String FEAT_SUFFIX = "#f";
    String SQUARED_FEAT_SUFFIX = "#f2";
    String MIN_FEAT_SUFFIX = "#m";
    String SIZE_FEAT_SUFFIX = "#d";
    String TERM_SIZE_FEAT_SUFFIX = "#t";
    int FREQUENT_TERMS = 1000; // tf required for a term to be considered "frequent"

    void close();

    // returns feature; if feature isn't found, returns -1
    double getFeature(String keyStr);

    void putFeature(String keyStr, double val, long frequency);

    // add val to the keyStr feature if it exists already; otherwise, create the feature
    void addValFeature(String keyStr, double val, long frequency);
}
