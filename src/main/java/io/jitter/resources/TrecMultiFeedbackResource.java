package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.search.SelectionFeedbackDocumentsResponse;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
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
public class TrecMultiFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecMultiFeedbackResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;
    private final TailyManager tailyManager;

    public TrecMultiFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager, ShardsManager shardsManager, TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
        this.tailyManager = tailyManager;
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
                                          @QueryParam("sFuture") @DefaultValue("true") BooleanParam sFuture,
                                          @QueryParam("method") @DefaultValue("crcsexp") String method,
                                          @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                          @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                          @QueryParam("v") @DefaultValue("10") IntParam v,
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
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);

        long startTime = System.currentTimeMillis();

        SelectionTopDocuments selectResults = null;

        Map<String, Double> selectedSources;
        Map<String, Double> selectedTopics;
        if ("taily".equalsIgnoreCase(method)) {
            selectedSources = tailyManager.select(query, v.get());
            selectedTopics = tailyManager.selectTopics(query, v.get());
        } else {
            selectResults = selectionManager.search(maxId, epoch, sLimit, sRetweets, sFuture, query, epochs);
            SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
            selectedSources = selectionManager.select(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
            selectedTopics = selectionManager.selectTopics(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
        }

        Set<String> fbSourcesEnabled = Sets.newHashSet(Iterables.limit(selectedSources.keySet(), fbCols.get()));
        Set<String> fbTopicsEnabled = Sets.newHashSet(Iterables.limit(selectedTopics.keySet(), fbCols.get()));

        Set<String> selected;
        if (!topic.isPresent()) {
            selected = !fbUseSources.get() ? selectedTopics.keySet() : selectedSources.keySet();
        } else {
            selected = Sets.newHashSet(topic.get());
        }

        SelectionTopDocuments shardResults = shardsManager.search(maxId, epoch, sRetweets, sFuture, fbDocs, fbUseSources, query, epochs, selected);
        
        if (shardResults.totalHits > 0) {
            FeatureVector queryFV = buildQueryFV(query);
            FeatureVector fbVector = buildFbVector(fbDocs.get(), fbTerms.get(), fbWeight, queryFV, shardResults, trecMicroblogAPIWrapper.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());
            query = buildFeedbackQuery(fbVector);

            if (fbUseSources.get()) {
                logger.info("Sources: {}\n fbDocs: {} Feature Vector:\n{}", fbSourcesEnabled != null ? Joiner.on(", ").join(fbSourcesEnabled) : "all", shardResults.scoreDocs.size(), fbVector.toString());
            } else {
                logger.info("Topics: {}\n fbDocs: {} Feature Vector:\n{}", fbTopicsEnabled != null ? Joiner.on(", ").join(fbTopicsEnabled) : "all", shardResults.scoreDocs.size(), fbVector.toString());
            }
        }

        TopDocuments results = trecMicroblogAPIWrapper.search(limit, retweets, maxId, query);

        long endTime = System.currentTimeMillis();

        int totalFbDocs = shardResults.totalHits;
        int totalHits = results != null ? results.totalHits : 0;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionFeedbackDocumentsResponse documentsResponse = new SelectionFeedbackDocumentsResponse(selectedSources, selectedTopics, method, totalFbDocs, fbTerms.get(), 0, selectResults, shardResults, results);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
