package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.search.SelectionFeedbackDocumentsResponse;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trecmf")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecMultiFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecMultiFeedbackResource.class);

    private static final StopperTweetAnalyzer analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, false);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;

    public TrecMultiFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager, ShardsManager shardsManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
    }

    @GET
    @Timed
    public SelectionSearchResponse search(@QueryParam("q") Optional<String> q,
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

        long startTime = System.currentTimeMillis();

        SelectionTopDocuments selectResults = null;
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
        Map<String, Double> selectedSources = selectionManager.select(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
        Map<String, Double> selectedTopics = selectionManager.selectTopics(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());

        Set<String> fbSourcesEnabled = Sets.newHashSet(Iterables.limit(selectedSources.keySet(), fbCols.get()));
        Set<String> fbTopicsEnabled = Sets.newHashSet(Iterables.limit(selectedTopics.keySet(), fbCols.get()));

        Set<String> selected = !fbUseSources.get() ? selectedTopics.keySet() : selectedSources.keySet();

        SelectionTopDocuments shardResults = null;
        if (q.isPresent()) {
            if (maxId.isPresent()) {
                shardResults = shardsManager.search(!fbUseSources.get(), selected, query, fbDocs.get(), !retweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                shardResults = shardsManager.search(!fbUseSources.get(), selected, query, fbDocs.get(), !retweets.get(), epochs[0], epochs[1]);
            } else {
                shardResults = shardsManager.search(!fbUseSources.get(), selected, query, fbDocs.get(), !retweets.get());
            }
        }
        
        if (shardResults.totalHits > 0) {
            FeatureVector queryFV = new FeatureVector(null);
            for (String term : AnalyzerUtils.analyze(analyzer, query)) {
                if (term.isEmpty())
                    continue;
                if ("AND".equals(term) || "OR".equals(term))
                    continue;
                queryFV.addTerm(term.toLowerCase(Locale.ROOT), 1.0);
            }
            queryFV.normalizeToOne();

            FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
            fb.setOriginalQueryFV(queryFV);
            fb.setRes(shardResults.scoreDocs);
            fb.build(trecMicroblogAPIWrapper.getStopper());

            FeatureVector fbVector = fb.asFeatureVector();
            fbVector.pruneToSize(fbTerms.get());
            fbVector.normalizeToOne();
            fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

            if (fbUseSources.get()) {
                logger.info("Sources: {}\n fbDocs: {} Feature Vector:\n{}", fbSourcesEnabled != null ? Joiner.on(", ").join(fbSourcesEnabled) : "all", shardResults.scoreDocs.size(), fbVector.toString());
            } else {
                logger.info("Topics: {}\n fbDocs: {} Feature Vector:\n{}", fbTopicsEnabled != null ? Joiner.on(", ").join(fbTopicsEnabled) : "all", shardResults.scoreDocs.size(), fbVector.toString());
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

        TopDocuments results = null;
        if (q.isPresent()) {
            if (maxId.isPresent()) {
                results = trecMicroblogAPIWrapper.search(query, maxId.get(), limit.get(), !retweets.get());
            } else {
                results = trecMicroblogAPIWrapper.search(query, Long.MAX_VALUE, limit.get(), !retweets.get());
            }
        }

        long endTime = System.currentTimeMillis();

        int totalFbDocs = shardResults.totalHits;
        int totalHits = results != null ? results.totalHits : 0;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionFeedbackDocumentsResponse documentsResponse = new SelectionFeedbackDocumentsResponse(selectedSources, selectedTopics, method, totalFbDocs, fbTerms.get(), 0, selectResults, shardResults, results);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
