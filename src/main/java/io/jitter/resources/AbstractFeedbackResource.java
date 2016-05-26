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

    private static final Analyzer ANALYZER = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, true);

    FeatureVector buildFeedbackFV(int fbDocs, int fbTerms, TopDocuments results, Stopper stopper, CollectionStats collectionStats) throws IOException {
        TweetFeedbackRelevanceModel fb = new TweetFeedbackRelevanceModel(stopper);
        fb.setCollectionStats(collectionStats);
        fb.setMaxQueryTerms(fbTerms);
//        logger.info(fb.describeParams());
//        fb.setOriginalQueryFV(queryFV);

        return fb.like(results.scoreDocs.subList(0, Math.min(fbDocs, results.scoreDocs.size())));
    }

    FeatureVector buildQueryFV(String query) throws ParseException {
        FeatureVector queryFV = new FeatureVector();
        for (String term : AnalyzerUtils.analyze(ANALYZER, query)) {
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
