package io.jitter.resources;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.TweetFeedbackRelevanceModel;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Stopper;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFeedbackResource.class);

    FeatureVector buildBootstrapFeedbackFV(int fbDocs, int fbTerms, TopDocuments results, Stopper stopper, CollectionStats collectionStats) throws IOException {
        TweetFeedbackRelevanceModel fb = new TweetFeedbackRelevanceModel(stopper);
        fb.setCollectionStats(collectionStats);
        fb.setMaxQueryTerms(fbTerms);
//        logger.info(fb.describeParams());
//        fb.setOriginalQueryFV(queryFV);

        List<Document> documents = results.scoreDocs.subList(0, Math.min(fbDocs, results.scoreDocs.size()));
        FeatureVector fbVector = null;
        int B = 30;
        for (int i = 0; i < B; i++) {
            List<Document> sample = sample(fbDocs, documents);
            FeatureVector like = fb.like(sample);
            if (fbVector == null) {
                fbVector = like;
            } else {
                fbVector = FeatureVector.add(fbVector, like);
            }
        }

        fbVector.pruneToSize(fbTerms);
        fbVector.scaleToUnitL1Norm();
        return fbVector;
    }

    private List<Document> sample(int fbDocs, List<Document> relDocs) {
        int numDocs = relDocs.size();
        List<Pair<Integer, Double>> probabilities = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            // TODO: Fix Lucene QL scores
            // double prob = Math.exp(relDocs.get(i).getRsv());
            probabilities.add(new Pair<>(i, 1.0/(i+1)));
        }

        EnumeratedDistribution<Integer> dist = new EnumeratedDistribution<>(probabilities);
        List<Document> sample = new ArrayList<>(fbDocs);
        for (int i = 0; i < fbDocs; i++) {
            int pos = dist.sample();
            Document doc = relDocs.get(pos);
            sample.add(doc);
        }
        return sample;
    }

    FeatureVector buildFeedbackFV(int fbDocs, int fbTerms, TopDocuments results, Stopper stopper, CollectionStats collectionStats) throws IOException {
        TweetFeedbackRelevanceModel fb = new TweetFeedbackRelevanceModel(stopper);
        fb.setCollectionStats(collectionStats);
        fb.setMaxQueryTerms(fbTerms);
//        logger.info(fb.describeParams());
//        fb.setOriginalQueryFV(queryFV);

        return fb.like(results.scoreDocs.subList(0, Math.min(fbDocs, results.scoreDocs.size())));
    }

    FeatureVector buildQueryFV(String query, Stopper stopper) throws ParseException {
        Analyzer analyzer;
        if (stopper == null || stopper.asSet().isEmpty()) {
            analyzer = new TweetAnalyzer(CharArraySet.EMPTY_SET);
        } else {
            CharArraySet charArraySet = new CharArraySet(stopper.asSet(), true);
            analyzer = new TweetAnalyzer(charArraySet);
        }
        FeatureVector queryFV = new FeatureVector();
        for (String term : AnalyzerUtils.analyze(analyzer, query)) {
            if (!term.isEmpty()) {
                queryFV.addFeatureWeight(term, 1f);
            }
        }
        queryFV.scaleToUnitL1Norm();
        return queryFV;
    }

    String buildQuery(FeatureVector fbVector) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> terms = fbVector.iterator();
        while (terms.hasNext()) {
            String term = terms.next();
            double prob = fbVector.getFeatureWeight(term);
            builder.append(term).append("^").append(Math.abs(prob)).append(" ");
        }
        return builder.toString().trim();
    }

    FeatureVector interpruneFV(int fbTerms, float fbWeight, FeatureVector fv1, FeatureVector fv2) {
        FeatureVector fv = FeatureVector.interpolate(fv1, fv2, fbWeight);
        fv.pruneToSize(fbTerms);
        fv.scaleToUnitL1Norm();
        return fv;
    }
}
