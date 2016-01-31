package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.FeedbackDocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.FeedbackRelevanceModel;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
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
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trecselfb")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecSelectionRM3FeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecSelectionRM3FeedbackResource.class);

    private static final StopperTweetAnalyzer analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, false);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;
    private final TailyManager tailyManager;

    public TrecSelectionRM3FeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager, ShardsManager shardsManager, TailyManager tailyManager) throws IOException {
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
    public SearchResponse search(@QueryParam("q") Optional<String> q,
                                 @QueryParam("fq") Optional<String> fq,
                                 @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                 @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                 @QueryParam("maxId") Optional<Long> maxId,
                                 @QueryParam("epoch") Optional<String> epoch,
                                 @QueryParam("sLimit") @DefaultValue("50") IntParam sLimit,
                                 @QueryParam("sRetweets") @DefaultValue("true") BooleanParam sRetweets,
                                 @QueryParam("sFuture") @DefaultValue("false") BooleanParam sFuture,
                                 @QueryParam("method") @DefaultValue("crcsexp") String method,
                                 @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                 @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                 @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                 @QueryParam("v") @DefaultValue("10") IntParam v,
                                 @QueryParam("fbDocs") @DefaultValue("50") IntParam fbDocs,
                                 @QueryParam("fbTerms") @DefaultValue("20") IntParam fbTerms,
                                 @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                 @QueryParam("fbCols") @DefaultValue("3") IntParam fbCols,
                                 @QueryParam("fbUseSources") @DefaultValue("false") BooleanParam fbUseSources,
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");

        long[] epochs = new long[2];
        if (epoch.isPresent()) {
            epochs = Epochs.parseEpochRange(epoch.get());
        }

        long startTime = System.currentTimeMillis();

        SelectionTopDocuments selectResults = null;

        Map<String, Double> selectedSources;
        Map<String, Double> selectedTopics;
        if ("taily".equalsIgnoreCase(method)) {
            selectedSources = tailyManager.select(query, v.get());
            selectedTopics = tailyManager.selectTopics(query, v.get());
        } else {
            if (q.isPresent()) {
                if (!sFuture.get()) {
                    if (maxId.isPresent()) {
                        selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), maxId.get());
                    } else if (epoch.isPresent()) {
                        selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), epochs[0], epochs[1]);
                    } else {
                        selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get());
                    }
                } else {
                    selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get());
                }
            }
            SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
            selectedSources = selectionManager.select(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
            selectedTopics = selectionManager.selectTopics(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
        }
        Set<String> selected = !fbUseSources.get() ? selectedTopics.keySet() : selectedSources.keySet();


        SelectionTopDocuments shardResults = null;
        if (q.isPresent()) {
            if (!sFuture.get()) {
                if (maxId.isPresent()) {
                    shardResults = shardsManager.search(!fbUseSources.get(), selected, query, limit.get(), !retweets.get(), maxId.get());
                } else if (epoch.isPresent()) {
                    shardResults = shardsManager.search(!fbUseSources.get(), selected, query, limit.get(), !retweets.get(), epochs[0], epochs[1]);
                } else {
                    shardResults = shardsManager.search(!fbUseSources.get(), selected, query, limit.get(), !retweets.get());
                }
            } else {
                shardResults = shardsManager.search(!fbUseSources.get(), selected, query, limit.get(), !retweets.get());
            }
        }

        if (fbDocs.get() > 0 && fbTerms.get() > 0) {
            FeatureVector queryFV = new FeatureVector(null);
            for (String term : AnalyzerUtils.analyze(analyzer, query)) {
                if (term.isEmpty())
                    continue;
                if ("AND".equals(term) || "OR".equals(term))
                    continue;
                queryFV.addTerm(term.toLowerCase(Locale.ROOT), 1.0);
            }
            queryFV.normalizeToOne();

            // cap results
            shardResults.scoreDocs = shardResults.scoreDocs.subList(0, Math.min(fbDocs.get(), shardResults.scoreDocs.size()));

            FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
            fb.setOriginalQueryFV(queryFV);
            fb.setRes(shardResults.scoreDocs);
            fb.build(trecMicroblogAPIWrapper.getStopper());

            FeatureVector fbVector = fb.asFeatureVector();
            fbVector.pruneToSize(fbTerms.get());
            fbVector.normalizeToOne();
            fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

            logger.info("fbDocs: {} Feature Vector:\n{}", shardResults.scoreDocs.size(), fbVector.toString());

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

        int totalFbDocs = shardResults != null ? shardResults.totalHits : 0;
        int totalHits = results != null ? results.totalHits : 0;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        FeedbackDocumentsResponse documentsResponse = new FeedbackDocumentsResponse(totalFbDocs, fbTerms.get(), totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
