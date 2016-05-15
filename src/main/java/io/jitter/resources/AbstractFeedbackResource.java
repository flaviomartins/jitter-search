package io.jitter.resources;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.TweetFeedbackRelevanceModel;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFeedbackResource.class);

    private static final StopperTweetAnalyzer analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, true, false, true);

    FeatureVector buildFbVector(int fbDocs, int fbTerms, double fbWeight, FeatureVector queryFV, TopDocuments selectResults, Stopper stopper, CollectionStats collectionStats) throws IOException {
        // cap results
        selectResults.scoreDocs = selectResults.scoreDocs.subList(0, Math.min(fbDocs, selectResults.scoreDocs.size()));

        Analyzer analyzer;
        if (stopper == null || stopper.asSet().size() == 0) {
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, true, false, true);
        } else {
            CharArraySet charArraySet = new CharArraySet(Version.LUCENE_43, stopper.asSet(), true);
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, charArraySet, true, false, true);
        }

        TweetFeedbackRelevanceModel fb = new TweetFeedbackRelevanceModel(analyzer);
        if (stopper != null) {
            fb.setStopWords(stopper.asSet());
        }
        fb.setCollectionStats(collectionStats);
        fb.setMaxQueryTerms(fbTerms);
        logger.info(fb.describeParams());
//        fb.setOriginalQueryFV(queryFV);
        FeatureVector fbVector = fb.like(selectResults.scoreDocs);
        fbVector = FeatureVector.interpolate(queryFV, fbVector, (float)fbWeight); // ORIG_QUERY_WEIGHT
        fbVector.pruneToSize(fbTerms);
        fbVector.scaleToUnitL1Norm();

        logger.info("fbDocs: {} Feature Vector:\n{}", selectResults.scoreDocs.size(), fbVector.toString());
        return fbVector;
    }

    FeatureVector buildQueryFV(String query) throws ParseException {
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
            builder.append(term).append("^").append(prob).append(" ");
        }
        return builder.toString().trim();
    }
}
