package org.novasearch.jitter.core.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.DefaultSimilarity;

public class IDFSimilarity extends DefaultSimilarity {

    @Override
    public float lengthNorm(FieldInvertState state) {
        final int numTerms;
        if (discountOverlaps)
            numTerms = state.getLength() - state.getNumOverlap();
        else
            numTerms = state.getLength();
        return state.getBoost() * (numTerms > 0 ? 1.0f : 0.0f);
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
