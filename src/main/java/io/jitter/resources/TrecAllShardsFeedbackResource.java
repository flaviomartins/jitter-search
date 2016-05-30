package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.search.SelectionFeedbackDocumentsResponse;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.document.FeatureVector;
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

@Path("/trec/shardsfb")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecAllShardsFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecAllShardsFeedbackResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final ShardsManager shardsManager;

    public TrecAllShardsFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, ShardsManager shardsManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(shardsManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
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
                                          @QueryParam("sFuture") @DefaultValue("true") BooleanParam sFuture,
                                          @QueryParam("method") @DefaultValue("crcsexp") String method,
                                          @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                          @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                          @QueryParam("v") @DefaultValue("10") IntParam v,
                                          @QueryParam("reScore") @DefaultValue("false") BooleanParam reScore,
                                          @QueryParam("fbDocs") @DefaultValue("50") IntParam fbDocs,
                                          @QueryParam("fbTerms") @DefaultValue("20") IntParam fbTerms,
                                          @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                          @QueryParam("fbCols") @DefaultValue("3") IntParam fbCols,
                                          @QueryParam("fbMerge") @DefaultValue("false") BooleanParam fbMerge,
                                          @QueryParam("topics") @DefaultValue("true") BooleanParam topics,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);

        long startTime = System.currentTimeMillis();

        SelectionTopDocuments shardResults = shardsManager.search(maxId, epoch, sRetweets.get(), sFuture.get(), limit.get(), topics.get(), query, epochs, null);
        shardResults.scoreDocs = shardResults.scoreDocs.subList(0, Math.min(fbDocs.get(), shardResults.scoreDocs.size()));

        FeatureVector shardsFV = null;
        if (shardResults.totalHits > 0) {
            shardsFV = buildFeedbackFV(fbDocs.get(), fbTerms.get(), shardResults, shardsManager.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());
        }

        FeatureVector feedbackFV = null;
        FeatureVector fbVector;
        Sets.SetView<String> fbFeatures = null;
        double fbFeaturesSize = 0;
        double fbJaccSimilarity = 0;
        if (fbMerge.get()) {
            TopDocuments selectResults = trecMicroblogAPIWrapper.search(limit, maxId, sRetweets, sFuture.get(), query);
            feedbackFV = buildFeedbackFV(fbDocs.get(), fbTerms.get(), selectResults, trecMicroblogAPIWrapper.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());

            HashSet<String> feedbackFeatures = Sets.newHashSet(feedbackFV.getFeatures());
            HashSet<String> shardsFeatures = Sets.newHashSet(shardsFV.getFeatures());
            fbFeatures = Sets.intersection(shardsFeatures, feedbackFeatures);
            fbFeaturesSize = (double) fbFeatures.size();
            fbJaccSimilarity = fbFeaturesSize / (shardsFeatures.size() + feedbackFeatures.size() - fbFeaturesSize);
            logger.warn("FV IntersectionSize: {}", fbFeaturesSize);
            logger.warn("FV JaccardSimilarity: {}", fbJaccSimilarity);

            fbVector = interpruneFV(fbTerms.get(), fbWeight.floatValue(), shardsFV, feedbackFV);
        } else {
            fbVector = shardsFV;
        }

        FeatureVector queryFV = buildQueryFV(query, trecMicroblogAPIWrapper.getStopper());
        fbVector = interpruneFV(fbTerms.get(), fbWeight.floatValue(), queryFV, fbVector);

        logger.info("Selected: all\n fbDocs: {} Feature Vector:\n{}", shardResults.scoreDocs.size(), fbVector.toString());

        query = buildQuery(fbVector);

        TopDocuments results = trecMicroblogAPIWrapper.search(limit, retweets, maxId, query);

        long endTime = System.currentTimeMillis();

        int totalFbDocs = shardResults.totalHits;
        int totalHits = results != null ? results.totalHits : 0;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionFeedbackDocumentsResponse documentsResponse = new SelectionFeedbackDocumentsResponse(null, null, method, totalFbDocs, fbTerms.get(), shardsFV.getMap(), feedbackFV != null ? feedbackFV.getMap() : null, fbVector.getMap(), 0, null, shardResults, results);
        if (fbMerge.get()) {
            documentsResponse.setFbFeatures(fbFeatures);
            documentsResponse.setFbFeaturesSize(fbFeaturesSize);
            documentsResponse.setFbJaccSimilarity(fbJaccSimilarity);
        }

        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
