package io.jitter.core.similarities;

import org.apache.lucene.search.similarities.ClassicSimilarity;

public class LtcSimilarity extends ClassicSimilarity {

    @Override
    public float tf(float freq) {
        return 1 + (float) Math.log(freq);
    }

    @Override
    public String toString() {
        return "LtcSimilarity";
    }

}
