package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.Document;
import io.jitter.api.search.SelectDocumentsResponse;
import io.jitter.api.search.SelectSearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.feedback.FeedbackRelevanceModel;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.methods.RankS;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.twittertools.api.TResultWrapper;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trecmf")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecMultiFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecMultiFeedbackResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final SelectionManager selectionManager;

    public TrecMultiFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.selectionManager = selectionManager;
    }

    @GET
    @Timed
    public SelectSearchResponse search(@QueryParam("q") Optional<String> q,
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
                                       @QueryParam("topic") Optional<String> topic,
                                       @QueryParam("fbDocs") @DefaultValue("50") IntParam fbDocs,
                                       @QueryParam("fbTerms") @DefaultValue("20") IntParam fbTerms,
                                       @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                       @QueryParam("fbTopics") @DefaultValue("3") IntParam fbTopics,
                                       @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        List<Document> selectResults = null;
        List<TResultWrapper> results = null;

        long startTime = System.currentTimeMillis();

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

        Map<String, Double> rankedSources = selectionManager.getRanked(selectionMethod, selectResults);

        Map<String, Double> sources = new LinkedHashMap<>();
        // rankS has its own limit mechanism
        if (RankS.class.getSimpleName().equals(methodName)) {
            for (Map.Entry<String, Double> entry : rankedSources.entrySet()) {
                if (entry.getValue() < minRanks)
                    break;
                sources.put(entry.getKey(), entry.getValue());
            }
        } else { // hard limit
            int i = 0;
            for (Map.Entry<String, Double> entry : rankedSources.entrySet()) {
                i++;
                if (i > maxCol.get())
                    break;
                sources.put(entry.getKey(), entry.getValue());
            }
        }

        Map<String, Double> rankedTopics = selectionManager.getRankedTopics(selectionMethod, selectResults);

        Map<String, Double> topics = new LinkedHashMap<>();
        // rankS has its own limit mechanism
        if (RankS.class.getSimpleName().equals(methodName)) {
            for (Map.Entry<String, Double> entry : rankedTopics.entrySet()) {
                if (entry.getValue() < minRanks)
                    break;
                topics.put(entry.getKey(), entry.getValue());
            }
        } else { // hard limit
            int i = 0;
            for (Map.Entry<String, Double> entry : rankedTopics.entrySet()) {
                i++;
                if (i > maxCol.get())
                    break;
                topics.put(entry.getKey(), entry.getValue());
            }
        }

        if (topics.size() > 0) {
            Iterable<String> fbTopicsEnabled = Iterables.limit(topics.keySet(), fbTopics.get());
            selectResults = selectionManager.filterTopics(fbTopicsEnabled, selectResults);
//            selectResults = selectionManager.reScoreSelected(Iterables.limit(topics.entrySet(), fbTopics.get()), selectResults);

            FeatureVector queryFV = new FeatureVector(null);
            for (String term : AnalyzerUtils.analyze(new StopperTweetAnalyzer(Version.LUCENE_43, false), query)) {
                if ("AND".equals(term) || "OR".equals(term))
                    continue;
                queryFV.addTerm(term.toLowerCase(), 1.0);
            }
            queryFV.normalizeToOne();

            // cap results
            selectResults = selectResults.subList(0, Math.min(fbDocs.get(), selectResults.size()));

            FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
            fb.setOriginalQueryFV(queryFV);
            fb.setRes(selectResults);
            fb.build(trecMicroblogAPIWrapper.getStopper());

            FeatureVector fbVector = fb.asFeatureVector();
            fbVector.pruneToSize(fbTerms.get());
            fbVector.normalizeToOne();
            fbVector = FeatureVector.interpolate(queryFV, fbVector, fbWeight); // ORIG_QUERY_WEIGHT

            logger.info("Feature Vector for topics {}:\n{}", Joiner.on(", ").join(fbTopicsEnabled), fbVector.toString());

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
                results = trecMicroblogAPIWrapper.search(query, maxId.get(), limit.get(), !retweets.get());
            } else {
                results = trecMicroblogAPIWrapper.search(query, Long.MAX_VALUE, limit.get(), !retweets.get());
            }
        }

        long endTime = System.currentTimeMillis();

        int totalHits = results != null ? results.size() : 0;

        logger.info(String.format("%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectDocumentsResponse documentsResponse = new SelectDocumentsResponse(sources, topics, methodName, totalHits, 0, selectResults, results);
        return new SelectSearchResponse(responseHeader, documentsResponse);
    }
}
