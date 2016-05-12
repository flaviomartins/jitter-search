package io.jitter.resources;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.FeedbackRelevanceModel;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.KeyValuePair;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFeedbackResource.class);

    private static final StopperTweetAnalyzer analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, false);

    String buildFeedbackQuery(FeatureVector fbVector) {
        NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
        DecimalFormat df = (DecimalFormat)nf;
        df.applyPattern("#.#########");
        StringBuilder b = new StringBuilder();
        List<KeyValuePair> kvpList = fbVector.getOrderedFeatures();
        Iterator<KeyValuePair> it = kvpList.iterator();
        while (it.hasNext()) {
            KeyValuePair pair = it.next();
            b.append('"').append(pair.getKey()).append('"').append("^").append(df.format(pair.getScore())).append(" ");
        }
        return b.toString();
    }

    FeatureVector buildFbVector(int fbDocs, int fbTerms, double fbWeight, FeatureVector queryFV, TopDocuments selectResults, Stopper stopper, CollectionStats collectionStats) {
        // cap results
        selectResults.scoreDocs = selectResults.scoreDocs.subList(0, Math.min(fbDocs, selectResults.scoreDocs.size()));

        Analyzer analyzer;
        if (stopper == null || stopper.asSet().size() == 0) {
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, false, false, true);
        } else {
            CharArraySet charArraySet = new CharArraySet(Version.LUCENE_43, stopper.asSet(), true);
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, charArraySet, false, false, true);
        }

        FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
        fb.setOriginalQueryFV(queryFV);
        fb.setRes(selectResults.scoreDocs);
        fb.build(analyzer);
        fb.idfFix(collectionStats);

        FeatureVector fbVector = fb.asFeatureVector();
        fbVector.pruneToSize(fbTerms);
        fbVector.normalizeToOne();
        fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

        logger.info("fbDocs: {} Feature Vector:\n{}", selectResults.scoreDocs.size(), fbVector.toString());
        return fbVector;
    }

    FeatureVector buildQueryFV(String query) {
        FeatureVector queryFV = new FeatureVector();
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
