package io.jitter.resources;

import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.FeedbackRelevanceModel;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Locale;

public class AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFeedbackResource.class);

    private static final StopperTweetAnalyzer analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, false);

    String buildFeedbackQuery(FeatureVector fbVector) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> terms = fbVector.iterator();
        while (terms.hasNext()) {
            String term = terms.next();
            double prob = fbVector.getFeatureWeight(term);
            if (prob < 0)
                continue;
            builder.append('"').append(term).append('"').append("^").append(prob).append(" ");
        }
        return builder.toString().trim();
    }

    FeatureVector buildFbVector(int fbDocs, int fbTerms, double fbWeight, FeatureVector queryFV, TopDocuments selectResults, Stopper stopper) {
        // cap results
        selectResults.scoreDocs = selectResults.scoreDocs.subList(0, Math.min(fbDocs, selectResults.scoreDocs.size()));

        FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
        fb.setOriginalQueryFV(queryFV);
        fb.setRes(selectResults.scoreDocs);
        fb.build(stopper);

        FeatureVector fbVector = fb.asFeatureVector();
        fbVector.pruneToSize(fbTerms);
        fbVector.normalizeToOne();
        fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

        logger.info("fbDocs: {} Feature Vector:\n{}", selectResults.scoreDocs.size(), fbVector.toString());
        return fbVector;
    }

    FeatureVector buildQueryFV(String query) {
        FeatureVector queryFV = new FeatureVector(null);
        for (String term : AnalyzerUtils.analyze(analyzer, query)) {
            if (term.isEmpty())
                continue;
            if ("AND".equals(term) || "OR".equals(term))
                continue;
            queryFV.addTerm(term.toLowerCase(Locale.ROOT), 1.0);
        }
        queryFV.normalizeToOne();
        return queryFV;
    }

}
