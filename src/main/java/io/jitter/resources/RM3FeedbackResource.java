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
import io.jitter.core.search.SearchManager;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
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
import java.util.concurrent.atomic.AtomicLong;

@Path("/fb")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class RM3FeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(RM3FeedbackResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;

    public RM3FeedbackResource(SearchManager searchManager) throws IOException {
        Preconditions.checkNotNull(searchManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
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

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        TopDocuments selectResults = null;
        TopDocuments results = null;

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                selectResults = searchManager.search(query, sLimit.get(), !retweets.get(), maxId.get());
            } else {
                selectResults = searchManager.search(query, sLimit.get(), !retweets.get(), Long.MAX_VALUE);
            }
        }

        if (fbDocs.get() > 0 && fbTerms.get() > 0) {
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
            fb.build(searchManager.getStopper());

            FeatureVector fbVector = fb.asFeatureVector();
            fbVector.pruneToSize(fbTerms.get());
            fbVector.normalizeToOne();
            fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

            logger.info("fbDocs: {} Feature Vector:\n{}", selectResults.scoreDocs.size(), fbVector.toString());

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
                results = searchManager.search(query, limit.get(), !retweets.get(), maxId.get());
            } else {
                results = searchManager.search(query, limit.get(), !retweets.get(), Long.MAX_VALUE);
            }
        }

        long endTime = System.currentTimeMillis();

        int totalFbDocs = selectResults != null ? selectResults.scoreDocs.size() : 0;
        int totalHits = results != null ? results.totalHits : 0;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        FeedbackDocumentsResponse documentsResponse = new FeedbackDocumentsResponse(totalFbDocs, fbTerms.get(), totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
