package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.FeedbackDocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.search.SearchManager;
import io.jitter.core.search.TopDocuments;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/fb")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class FeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(FeedbackResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;

    public FeedbackResource(SearchManager searchManager) throws IOException {
        Preconditions.checkNotNull(searchManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    public SearchResponse search(@QueryParam("q") Optional<String> q,
                                 @QueryParam("fq") Optional<String> fq,
                                 @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                 @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                 @QueryParam("maxId") Optional<Long> maxId,
                                 @QueryParam("epoch") Optional<String> epoch,
                                 @QueryParam("sLimit") @DefaultValue("1000") IntParam sLimit,
                                 @QueryParam("sRetweets") @DefaultValue("true") BooleanParam sRetweets,
                                 @QueryParam("sFuture") @DefaultValue("false") BooleanParam sFuture,
                                 @QueryParam("method") @DefaultValue("crcsexp") String method,
                                 @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                 @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                 @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                 @QueryParam("fbDocs") @DefaultValue("50") IntParam fbDocs,
                                 @QueryParam("fbTerms") @DefaultValue("20") IntParam fbTerms,
                                 @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);
        
        long startTime = System.currentTimeMillis();

        TopDocuments selectResults = searchManager.search(sLimit.get(), sRetweets.get(), sFuture.get(), maxId, epoch, query, epochs);
        selectResults.scoreDocs = selectResults.scoreDocs.subList(0, Math.min(fbDocs.get(), selectResults.scoreDocs.size()));

        FeatureVector fbVector = null;
        if (fbDocs.get() > 0 && fbTerms.get() > 0) {
            FeatureVector queryFV = buildQueryFV(query);
            FeatureVector feedbackFV = buildFeedbackFV(fbDocs.get(), fbTerms.get(), selectResults, searchManager.getStopper(), searchManager.getCollectionStats());
            fbVector = interpruneFV(fbTerms.get(), fbWeight.floatValue(), queryFV, feedbackFV);

            logger.info("\n fbDocs: {} Feature Vector:\n{}", selectResults.scoreDocs.size(), fbVector.toString());

            query = buildQuery(fbVector);
        }

        TopDocuments results = searchManager.search(limit.get(), retweets.get(), maxId, epoch, query, epochs);

        long endTime = System.currentTimeMillis();

        int totalFbDocs = selectResults != null ? selectResults.scoreDocs.size() : 0;
        int totalHits = results != null ? results.totalHits : 0;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        FeedbackDocumentsResponse documentsResponse = new FeedbackDocumentsResponse(totalFbDocs, fbTerms.get(), fbVector.getMap(), 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
