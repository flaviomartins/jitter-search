package io.jitter.core.document;

import java.util.LinkedHashMap;
import org.apache.commons.math3.linear.OpenMapRealVector;
import org.apache.commons.math3.linear.RealVectorFormat;

public class DocVector {

    public LinkedHashMap<String, Integer> terms;
    public OpenMapRealVector vector;

    public DocVector(LinkedHashMap terms) {
        this.terms = terms;
        this.vector = new OpenMapRealVector(terms.size());
    }

    public void setEntry(String term, int freq) {
        if (terms.containsKey(term)) {
            int pos = terms.get(term);
            vector.setEntry(pos, (double) freq);
        }
    }

    public void normalize() {
        double sum = vector.getL1Norm();
        vector = (OpenMapRealVector) vector.mapDivide(sum);
    }

    @Override
    public String toString() {
        RealVectorFormat formatter = new RealVectorFormat();
        return formatter.format(vector);
    }
}
