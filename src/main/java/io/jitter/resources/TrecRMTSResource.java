package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.RMTSDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.rerank.RMTSReranker;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.taily.TailyManager;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trec/rmts")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecRMTSResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecRMTSResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;
    private final TailyManager tailyManager;
    private final RMTSReranker rmtsReranker;

    public TrecRMTSResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager, ShardsManager shardsManager, TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
        this.tailyManager = tailyManager;
        this.rmtsReranker = new RMTSReranker("ltr-all.model");
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
                                          @QueryParam("numRerank") @DefaultValue("1000") IntParam numRerank,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);

        long startTime = System.currentTimeMillis();

        Selection selection;
        if ("taily".equalsIgnoreCase(method)) {
            selection = tailyManager.selection(query, v);
        } else {
            selection = selectionManager.selection(maxId, epoch, sLimit, sRetweets, sFuture, method, maxCol, minRanks, normalize, query, epochs);
        }
        Set<String> selected = !fbUseSources.get() ? selection.getTopics().keySet() : selection.getSources().keySet();

        SelectionTopDocuments shardResults = shardsManager.search(maxId, epoch, sRetweets, sFuture, fbDocs, fbUseSources, query, epochs, selected);

        // get the query epoch
        double currentEpoch = System.currentTimeMillis() / 1000L;
        double queryEpoch = epoch.isPresent() ? epochs[1] : currentEpoch;

        TopDocuments results = trecMicroblogAPIWrapper.search(limit, retweets, maxId, query);

//        NaiveLanguageFilter langFilter = new NaiveLanguageFilter("en");
//        langFilter.setResults(results.scoreDocs);
//        results.scoreDocs = langFilter.getFiltered();
        
        results.scoreDocs = rmtsReranker.score(query, queryEpoch, results.scoreDocs, shardResults.scoreDocs, trecMicroblogAPIWrapper.getCollectionStats(), limit.get(), numRerank.get());

        long endTime = System.currentTimeMillis();

        int totalHits = results.totalHits;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        RMTSDocumentsResponse documentsResponse = new RMTSDocumentsResponse(selection.getSources(), selection.getTopics(), method, 0, selection.getResults(), shardResults, results);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
