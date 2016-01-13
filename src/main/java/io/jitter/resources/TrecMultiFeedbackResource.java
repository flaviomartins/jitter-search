package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.search.SelectFeedbackDocumentsResponse;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectSearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.FeedbackRelevanceModel;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import io.jitter.core.utils.AnalyzerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trecmf")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecMultiFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecMultiFeedbackResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;

    public TrecMultiFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.selectionManager = selectionManager;
    }

    @GET
    @Timed
    public SelectSearchResponse search(@QueryParam("q") Optional<String> q,
                                       @QueryParam("fq") Optional<String> fq,
                                       @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                       @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                       @QueryParam("maxId") Optional<Long> maxId,
                                       @QueryParam("epoch") Optional<String> epoch,
                                       @QueryParam("sLimit") @DefaultValue("50") IntParam sLimit,
                                       @QueryParam("sRetweets") @DefaultValue("true") BooleanParam sRetweets,
                                       @QueryParam("method") @DefaultValue("crcsexp") String method,
                                       @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                       @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                       @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                       @QueryParam("reScore") @DefaultValue("false") BooleanParam reScore,
                                       @QueryParam("topic") Optional<String> topic,
                                       @QueryParam("fbDocs") @DefaultValue("50") IntParam fbDocs,
                                       @QueryParam("fbTerms") @DefaultValue("20") IntParam fbTerms,
                                       @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                       @QueryParam("fbCols") @DefaultValue("3") IntParam fbCols,
                                       @QueryParam("fbUseSources") @DefaultValue("false") BooleanParam fbUseSources,
                                       @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        TopDocuments selectResults = null;
        TopDocuments results = null;

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), epochs[0], epochs[1]);
            } else {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get());
            }
        }

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        String methodName = selectionMethod.getClass().getSimpleName();

        Map<String, Double> rankedSources = selectionManager.getRanked(selectionMethod, selectResults.scoreDocs, normalize.get());
        Map<String, Double> sources = selectionManager.limit(selectionMethod, rankedSources, maxCol.get(), minRanks);

        Map<String, Double> rankedTopics = selectionManager.getRankedTopics(selectionMethod, selectResults.scoreDocs, normalize.get());
        Map<String, Double> topics = selectionManager.limit(selectionMethod, rankedTopics, maxCol.get(), minRanks);

        Iterable<String> fbSourcesEnabled = null;
        Iterable<String> fbTopicsEnabled = null;

        if (fbUseSources.get()) {
            fbSourcesEnabled = Iterables.limit(sources.keySet(), fbCols.get());
            selectResults = selectionManager.filterCollections(fbSourcesEnabled, selectResults.scoreDocs);
        } else {
            fbTopicsEnabled = Iterables.limit(topics.keySet(), fbCols.get());
            selectResults = selectionManager.filterTopics(fbTopicsEnabled, selectResults.scoreDocs);
            if (reScore.get()) {
                selectResults = selectionManager.reScoreSelected(Iterables.limit(topics.entrySet(), fbCols.get()), selectResults.scoreDocs);
            }
        }

        if (sources.size() > 0 && topics.size() > 0) {
            FeatureVector queryFV = new FeatureVector(null);
            for (String term : AnalyzerUtils.analyze(new StopperTweetAnalyzer(Version.LUCENE_43, false), query)) {
                if (term.isEmpty())
                    continue;
                if ("AND".equals(term) || "OR".equals(term))
                    continue;
                queryFV.addTerm(term.toLowerCase(Locale.ROOT), 1.0);
            }
            queryFV.normalizeToOne();

            // cap results
            selectResults.scoreDocs = selectResults.scoreDocs.subList(0, Math.min(fbDocs.get(), selectResults.scoreDocs.size()));

            FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
            fb.setOriginalQueryFV(queryFV);
            fb.setRes(selectResults.scoreDocs);
            fb.build(trecMicroblogAPIWrapper.getStopper());

            FeatureVector fbVector = fb.asFeatureVector();
            fbVector.pruneToSize(fbTerms.get());
            fbVector.normalizeToOne();
            fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

            if (fbUseSources.get()) {
                logger.info("Sources: {}\n fbDocs: {} Feature Vector:\n{}", Joiner.on(", ").join(fbSourcesEnabled), selectResults.scoreDocs.size(), fbVector.toString());
            } else {
                logger.info("Topics: {}\n fbDocs: {} Feature Vector:\n{}", Joiner.on(", ").join(fbTopicsEnabled), selectResults.scoreDocs.size(), fbVector.toString());
            }

            StringBuilder builder = new StringBuilder();
            Iterator<String> terms = fbVector.iterator();
            while (terms.hasNext()) {
                String term = terms.next();
                double prob = fbVector.getFeatureWeight(term);
                if (prob < 0)
                    continue;
                builder.append('"').append(term).append('"').append("^").append(prob).append(" ");
            }
            query = builder.toString().trim();
        }

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                results = trecMicroblogAPIWrapper.search(query, maxId.get(), limit.get(), !retweets.get());
            } else {
                results = trecMicroblogAPIWrapper.search(query, Long.MAX_VALUE, limit.get(), !retweets.get());
            }
        }

        long endTime = System.currentTimeMillis();

        int totalFbDocs = selectResults.scoreDocs.size();
        int totalHits = results != null ? results.totalHits : 0;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectFeedbackDocumentsResponse documentsResponse = new SelectFeedbackDocumentsResponse(sources, topics, methodName, totalFbDocs, fbTerms.get(), totalHits, 0, selectResults, results);
        return new SelectSearchResponse(responseHeader, documentsResponse);
    }
}
