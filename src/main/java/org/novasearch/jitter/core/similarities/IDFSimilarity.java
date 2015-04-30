package org.novasearch.jitter.core.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.DefaultSimilarity;

public class IDFSimilarity extends DefaultSimilarity {

    @Override
    public float lengthNorm(FieldInvertState state) {
        return 1;
    }

    @Override
    public float tf(float freq) {
        return freq > 1 ? 1 : 0;
    }

    @Override
    public String toString() {
        return "IDFSimilarity";
    }

}
