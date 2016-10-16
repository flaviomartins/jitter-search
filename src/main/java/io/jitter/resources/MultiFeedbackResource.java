package io.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.caching.CacheControl;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.search.SelectionFeedbackDocumentsResponse;
import io.jitter.core.rerank.MaxTFFilter;
import io.jitter.core.rerank.RerankerCascade;
import io.jitter.core.rerank.RerankerContext;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.search.SearchManager;
import io.jitter.core.selection.SelectionManager;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/mf")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class MultiFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(MultiFeedbackResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;
    private final TailyManager tailyManager;

    public MultiFeedbackResource(SearchManager searchManager, SelectionManager selectionManager, ShardsManager shardsManager, TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(searchManager);
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
        this.tailyManager = tailyManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
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
                                          @QueryParam("fbMerge") @DefaultValue("false") BooleanParam fbMerge,
                                          @QueryParam("topics") @DefaultValue("true") BooleanParam topics,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);

        long startTime = System.currentTimeMillis();

        Selection selection;
        if ("taily".equalsIgnoreCase(method)) {
            selection = tailyManager.selection(query, v.get());
        } else {
            selection = selectionManager.selection(maxId, epoch, sLimit.get(), sRetweets.get(), sFuture.get(), method, maxCol.get(), minRanks, normalize.get(), query, epochs);
        }

        Set<String> selected;
        if (topic.isPresent()) {
            selected = Sets.newHashSet(topic.get());
        } else {
            Set<String> fbSourcesEnabled = Sets.newHashSet(Iterables.limit(selection.getSources().keySet(), fbCols.get()));
            Set<String> fbTopicsEnabled = Sets.newHashSet(Iterables.limit(selection.getTopics().keySet(), fbCols.get()));
            selected = topics.get() ? fbTopicsEnabled : fbSourcesEnabled;
        }

        SelectionTopDocuments shardResults = shardsManager.search(maxId, epoch, sRetweets.get(), sFuture.get(), limit.get(), topics.get(), query, epochs, selected);
        shardResults.scoreDocs = shardResults.scoreDocs.subList(0, Math.min(fbDocs.get(), shardResults.scoreDocs.size()));

        FeatureVector shardsFV = null;
        if (shardResults.totalHits > 0) {
            shardsFV = buildFeedbackFV(fbDocs.get(), fbTerms.get(), shardResults, shardsManager.getStopper(), searchManager.getCollectionStats());
        }

        FeatureVector feedbackFV = null;
        FeatureVector fbVector;
        if (fbMerge.get()) {
            TopDocuments selectResults = searchManager.search(limit.get(), retweets.get(), maxId, epoch, query, epochs);
            feedbackFV = buildFeedbackFV(fbDocs.get(), fbTerms.get(), selectResults, searchManager.getStopper(), searchManager.getCollectionStats());
            fbVector = interpruneFV(fbTerms.get(), fbWeight.floatValue(), shardsFV, feedbackFV);
        } else {
            fbVector = shardsFV;
        }

        FeatureVector queryFV = buildQueryFV(query, searchManager.getStopper());
        fbVector = interpruneFV(fbTerms.get(), fbWeight.floatValue(), queryFV, fbVector);

        logger.info("Selected: {}\n fbDocs: {} Feature Vector:\n{}", selected != null ? Joiner.on(", ").join(selected) : "all", shardResults.scoreDocs.size(), fbVector.toString());

        query = buildQuery(fbVector);

        // get the query epoch
        double currentEpoch = System.currentTimeMillis() / 1000L;
        double queryEpoch = epoch.isPresent() ? epochs[1] : currentEpoch;

        TopDocuments results = searchManager.search(limit.get(), retweets.get(), maxId, epoch, query, epochs);

        RerankerCascade cascade = new RerankerCascade();
        cascade.add(new MaxTFFilter(5));

        RerankerContext context = new RerankerContext(null, null, "MB000", query,
                queryEpoch, Lists.newArrayList(), IndexStatuses.StatusField.TEXT.name, null);
        results.scoreDocs = cascade.run(results.scoreDocs, context);

        long endTime = System.currentTimeMillis();

        int totalFbDocs = shardResults.totalHits;
        int totalHits = results.totalHits;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionFeedbackDocumentsResponse documentsResponse = new SelectionFeedbackDocumentsResponse(selection.getSources(), selection.getTopics(), method, totalFbDocs, fbTerms.get(), shardsFV.getMap(), feedbackFV != null ? feedbackFV.getMap() : null, fbVector.getMap(), 0, selection.getResults(), shardResults, results);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
