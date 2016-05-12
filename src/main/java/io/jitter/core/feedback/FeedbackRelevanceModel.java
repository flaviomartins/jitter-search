package io.jitter.core.feedback;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.KeyValuePair;
import io.jitter.core.utils.TweetUtils;
import jsat.text.stemming.PorterStemmer;
import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.util.*;


public class FeedbackRelevanceModel extends FeedbackModel {
    private double[] docWeights = null;

    private final PorterStemmer stemmer = new PorterStemmer();

    @Override
    public void build(Analyzer analyzer) {
        this.analyzer = analyzer;
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
            String cleanText = TweetUtils.clean(hit.getText());
            List<String> terms = AnalyzerUtils.analyze(analyzer, cleanText);
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

    public void idfFix(CollectionStats collectionStats) {
        try {
            int totalTerms = 0;
            if (collectionStats != null) {
                totalTerms = collectionStats.getTotalTerms();
            }

            if (totalTerms != 0) {
                for (KeyValuePair f : features) {
                    String stem = stemmer.stem(f.getKey());
                    double idfWeight = (double) totalTerms / (1.0 + collectionStats.getDF(stem));
                    f.setScore(f.getScore() * Math.log(idfWeight));
                }
            }
        } catch (IOException e) {
            // do nothing
        }
    }

    public void setDocWeights(double[] docWeights) {
        this.docWeights = docWeights;
    }


}
