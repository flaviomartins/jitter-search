package io.jitter.core.similarities;

import org.apache.lucene.search.similarities.DefaultSimilarity;

public class IDFSimilarity extends DefaultSimilarity {

    @Override
    public float tf(float freq) {
        return freq < 1 ? 0 : 1.0f;
    }

    @Override
    public String toString() {
        return "IDFSimilarity";
    }

}
