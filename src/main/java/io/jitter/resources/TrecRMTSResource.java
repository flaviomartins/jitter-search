package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.filter.NaiveLanguageFilter;
import io.jitter.core.rerank.RMTSReranker;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trecrmts")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecRMTSResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecRMTSResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;

    public TrecRMTSResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.selectionManager = selectionManager;
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
                                          @QueryParam("numRerank") @DefaultValue("1000") IntParam numRerank,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        SelectionTopDocuments selectResults = null;
        TopDocuments results = null;

        long[] epochs = new long[2];
        if (epoch.isPresent()) {
            epochs = Epochs.parseEpochRange(epoch.get());
        }

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
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

        Iterable<String> fbSourcesEnabled;
        Iterable<String> fbTopicsEnabled;

        if (fbUseSources.get()) {
            fbSourcesEnabled = Iterables.limit(sources.keySet(), fbCols.get());
            selectResults = selectionManager.filterCollections(fbSourcesEnabled, selectResults);
        } else {
            fbTopicsEnabled = Iterables.limit(topics.keySet(), fbCols.get());
            selectResults = selectionManager.filterTopics(fbTopicsEnabled, selectResults);
            if (reScore.get()) {
                selectResults = selectionManager.reScoreSelected(Iterables.limit(topics.entrySet(), fbCols.get()), selectResults.scoreDocs);
            }
        }

        // get the query epoch
        double currentEpoch = System.currentTimeMillis() / 1000L;
        double queryEpoch = epoch.isPresent() ? epochs[1] : currentEpoch;

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                results = trecMicroblogAPIWrapper.search(query, maxId.get(), limit.get(), !retweets.get());
//                score(results, queryEpoch);
            } else {
                results = trecMicroblogAPIWrapper.search(query, Long.MAX_VALUE, limit.get(), !retweets.get());
            }
        }

        NaiveLanguageFilter langFilter = new NaiveLanguageFilter("en");
        langFilter.setResults(results.scoreDocs);
        results.scoreDocs = langFilter.getFiltered();

        RMTSReranker rmtsReranker = new RMTSReranker(query, queryEpoch, results.scoreDocs, trecMicroblogAPIWrapper.getCollectionStats(), limit.get(), numRerank.get());
        results.scoreDocs = rmtsReranker.getReranked();

        long endTime = System.currentTimeMillis();

        int totalHits = results.totalHits;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionSearchDocumentsResponse documentsResponse = new SelectionSearchDocumentsResponse(sources, topics, methodName, 0, selectResults, results);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
