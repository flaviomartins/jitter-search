package io.jitter.core.similarities;

import org.apache.lucene.search.similarities.DefaultSimilarity;

public class LtcSimilarity extends DefaultSimilarity {

    @Override
    public float tf(float freq) {
        return 1 + (float) Math.log(freq);
    }

    @Override
    public String toString() {
        return "LtcSimilarity";
    }

}
