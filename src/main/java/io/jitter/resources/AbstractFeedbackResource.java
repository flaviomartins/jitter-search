package io.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.TweetFeedbackRelevanceModel;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFeedbackResource.class);

    private static final StopperTweetAnalyzer analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, false);
    private static final QueryParser QUERY_PARSER = new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer);

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

        TweetFeedbackRelevanceModel fb = new TweetFeedbackRelevanceModel(analyzer);
        fb.setCollectionStats(collectionStats);
        fb.setMinWordLen(2);
        fb.setMinTermFreq(0);
        fb.setMinDocFreq(11);
        logger.info(fb.describeParams());
        fb.setOriginalQueryFV(queryFV);
        fb.build(selectResults.scoreDocs);
        fb.idfFix();

        FeatureVector fbVector = fb.asFeatureVector();
        fbVector.pruneToSize(fbTerms);
        fbVector.normalizeToOne();
        fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

        logger.info("fbDocs: {} Feature Vector:\n{}", selectResults.scoreDocs.size(), fbVector.toString());
        return fbVector;
    }

    FeatureVector buildQueryFV(String query) throws ParseException {
        FeatureVector queryFV = new FeatureVector();
        Query q = QUERY_PARSER.parse(query.replaceAll(",", ""));
        Set<Term> queryTerms = new TreeSet<>();
        q.extractTerms(queryTerms);
        for (Term term : queryTerms) {
            String text = term.text();
            if (text.isEmpty())
                continue;
            queryFV.addTerm(text, 1.0);
        }
        queryFV.normalizeToOne();
        return queryFV;
    }

}
