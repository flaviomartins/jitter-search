package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.*;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
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
import java.util.concurrent.atomic.AtomicLong;

@Path("/trec/fb")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecFeedbackResource.class);

//    private static final Qrels qrels = new Qrels("/home/fmartins/IdeaProjects/microblog-search/microblog-search/data/qrels.microblog2014.txt");

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;

    public TrecFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
    }

    @GET
    @Timed
    public SearchResponse search(@QueryParam("qid") Optional<String> qid,
                                 @QueryParam("q") Optional<String> q,
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

        long startTime = System.currentTimeMillis();

        TopDocuments selectResults = trecMicroblogAPIWrapper.search(sLimit, maxId, sRetweets, sFuture.get(), query);
        selectResults.scoreDocs = selectResults.scoreDocs.subList(0, Math.min(fbDocs.get(), selectResults.scoreDocs.size()));


//        if (qid.isPresent()) {
//            QrelsReranker qrelsReranker = new QrelsReranker(selectResults.scoreDocs, qrels, qid.get().replaceFirst("^MB0*", ""));
//            selectResults.scoreDocs = qrelsReranker.getReranked();
//        }

//        NaiveLanguageFilter langFilter = new NaiveLanguageFilter("en");
//        langFilter.setResults(selectResults.scoreDocs);
//        selectResults.scoreDocs = langFilter.getFiltered();

        FeatureVector fbVector = null;
        if (fbDocs.get() > 0 && fbTerms.get() > 0) {
            FeatureVector queryFV = buildQueryFV(query);
            FeatureVector feedbackFV = buildFeedbackFV(fbDocs.get(), fbTerms.get(), selectResults, trecMicroblogAPIWrapper.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());
            fbVector = interpruneFV(fbTerms.get(), fbWeight.floatValue(), queryFV, feedbackFV);
            query = buildQuery(fbVector);
        }

        TopDocuments results = trecMicroblogAPIWrapper.search(limit, retweets, maxId, query);

        long endTime = System.currentTimeMillis();

        int totalFbDocs = selectResults != null ? selectResults.totalHits : 0;
        int totalHits = results != null ? results.totalHits : 0;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        FeedbackDocumentsResponse documentsResponse = new FeedbackDocumentsResponse(totalFbDocs, fbTerms.get(), fbVector.getMap(), totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
