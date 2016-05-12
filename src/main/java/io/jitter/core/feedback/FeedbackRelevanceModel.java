package io.jitter.core.feedback;

import io.jitter.api.search.Document;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.utils.KeyValuePair;

import java.util.*;


public class FeedbackRelevanceModel extends FeedbackModel {
    private double[] docWeights = null;

    public FeedbackRelevanceModel() {
        super();
    }

    public List<String> extractTerms(String text) {
        return Arrays.asList(text.split(" "));
    }

    @Override
    public void build() {
        Set<String> vocab = new HashSet<>();
        List<FeatureVector> fbDocVectors = new LinkedList<>();

        double[] scores = new double[relDocs.size()];
        int k = 0;
        Iterator<Document> hitIterator = relDocs.iterator();
        while (hitIterator.hasNext()) {
            Document hit = hitIterator.next();
            scores[k++] = hit.getRsv();
        }

        hitIterator = relDocs.iterator();
        while (hitIterator.hasNext()) {
            Document hit = hitIterator.next();
            List<String> terms = extractTerms(hit.getText());
            FeatureVector docVector = new FeatureVector(terms);
            vocab.addAll(docVector.getFeatures());
            fbDocVectors.add(docVector);
        }

        features = new LinkedList<>();
        for (String term : vocab) {
            double fbWeight = 0.0;

            Iterator<FeatureVector> docIT = fbDocVectors.iterator();
            k = 0;
            while (docIT.hasNext()) {
                double docWeight = 1.0;
                if (docWeights != null)
                    docWeight = docWeights[k];
                FeatureVector docVector = docIT.next();
                if (docVector.getLength() < 1)
                    continue;
                double docProb = docVector.getFeatureWeight(term) / docVector.getLength();
                docProb *= scores[k++] * docWeight;
                fbWeight += docProb;
            }

            fbWeight /= (double) fbDocVectors.size();

            KeyValuePair tuple = new KeyValuePair(term, fbWeight);
            features.add(tuple);
        }
    }

    public void setDocWeights(double[] docWeights) {
        this.docWeights = docWeights;
    }
}
