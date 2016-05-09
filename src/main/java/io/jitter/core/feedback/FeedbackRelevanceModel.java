package io.jitter.core.feedback;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.twittertools.api.TrecCollectionStats;
import io.jitter.core.utils.Stopper;
import io.jitter.core.utils.KeyValuePair;
import jsat.text.stemming.PorterStemmer;

import java.util.*;


public class FeedbackRelevanceModel extends FeedbackModel {
    private boolean stripNumbers = false;
    private double[] docWeights = null;

    private PorterStemmer stemmer = new PorterStemmer();

    @Override
    public void build(Stopper stopper) {
        this.build(stopper, null);
    }

    public void build(Stopper stopper, CollectionStats collectionStats) {
        this.stopper = stopper;
        try {
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
                FeatureVector docVector = new FeatureVector(hit.getText(), stopper);
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

                if (collectionStats != null) {
                    String stem = stemmer.stem(term);
                    double rm3IDF = TrecCollectionStats.DEFAULT_TERMS_SIZE / (double) collectionStats.getDF(stem);
                    fbWeight *= Math.log(rm3IDF);
                }

                fbWeight /= (double) fbDocVectors.size();

                KeyValuePair tuple = new KeyValuePair(term, fbWeight);
                features.add(tuple);
            }


        } catch (Exception e) {
            // do nothing
        }
    }

    public void setDocWeights(double[] docWeights) {
        this.docWeights = docWeights;
    }


}
