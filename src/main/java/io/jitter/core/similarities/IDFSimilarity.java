package io.jitter.core.similarities;

import org.apache.lucene.search.similarities.ClassicSimilarity;

public class IDFSimilarity extends ClassicSimilarity {

    @Override
    public float tf(float freq) {
        return freq < 1 ? 0 : 1.0f;
    }

    @Override
    public String toString() {
        return "IDFSimilarity";
    }

}
