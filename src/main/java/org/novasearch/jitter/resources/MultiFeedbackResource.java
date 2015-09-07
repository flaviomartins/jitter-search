package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.api.search.SelectDocumentsResponse;
import org.novasearch.jitter.api.search.SelectSearchResponse;
import org.novasearch.jitter.core.analysis.StopperTweetAnalyzer;
import org.novasearch.jitter.core.document.FeatureVector;
import org.novasearch.jitter.core.feedback.FeedbackRelevanceModel;
import org.novasearch.jitter.core.search.SearchManager;
import org.novasearch.jitter.core.selection.SelectionManager;
import org.novasearch.jitter.core.selection.methods.RankS;
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;
import org.novasearch.jitter.core.utils.AnalyzerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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

@Path("/mf")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class MultiFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(MultiFeedbackResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;
    private final SelectionManager selectionManager;

    public MultiFeedbackResource(SearchManager searchManager, SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(searchManager);
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
        this.selectionManager = selectionManager;
    }

    @GET
    @Timed
    public SelectSearchResponse search(@QueryParam("q") Optional<String> q,
                                 @QueryParam("fq") Optional<String> filterQuery,
                                 @QueryParam("limit") Optional<Integer> limit,
                                 @QueryParam("max_id") Optional<Long> max_id,
                                 @QueryParam("epoch") Optional<String> epoch_range,
                                 @QueryParam("filter_rt") Optional<Boolean> filter_rt,
                                 @QueryParam("method") Optional<String> method,
                                 @QueryParam("slimit") Optional<Integer> slimit,
                                 @QueryParam("max_col") Optional<Integer> max_col,
                                 @QueryParam("min_ranks") Optional<Double> min_ranks,
                                 @QueryParam("topic") Optional<String> topic,
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        int n = limit.or(30);
        long maxId = max_id.or(-1L);
        boolean filterRT = filter_rt.or(false);

        String methodText = method.or(selectionManager.getMethod());
        int s = slimit.or(50);
        int col_max = max_col.or(3);
        double ranks_min = min_ranks.or(1e-5);
        String topicName = topic.or("");

        long startTime = System.currentTimeMillis();

        List<Document> selectResults;

        if (max_id.isPresent()) {
            selectResults = selectionManager.search(query, s, filterRT, maxId);
        } else if (epoch_range.isPresent()) {
            long firstEpoch = 0L;
            long lastEpoch = Long.MAX_VALUE;
            String[] epochs = epoch_range.get().split("[: ]");
            try {
                if (epochs.length == 1) {
                    lastEpoch = Long.parseLong(epochs[0]);
                } else {
                    firstEpoch = Long.parseLong(epochs[0]);
                    lastEpoch = Long.parseLong(epochs[1]);
                }
            } catch (Exception e) {
                // pass
            }
            selectResults = selectionManager.search(query, s, filterRT, firstEpoch, lastEpoch);
        } else {
            selectResults = selectionManager.search(query, s, filterRT);
        }

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(methodText);
        String methodName = selectionMethod.getClass().getSimpleName();

        Map<String, Double> rankedSources = selectionManager.getRanked(selectionMethod, selectResults);

        Map<String, Double> sources = new LinkedHashMap<>();
        // rankS has its own limit mechanism
        if (RankS.class.getSimpleName().equals(methodName)) {
            for (Map.Entry<String, Double> entry : rankedSources.entrySet()) {
                if (entry.getValue() < ranks_min)
                    break;
                sources.put(entry.getKey(), entry.getValue());
            }
        } else { // hard limit
            int i = 0;
            for (Map.Entry<String, Double> entry : rankedSources.entrySet()) {
                i++;
                if (i > col_max)
                    break;
                sources.put(entry.getKey(), entry.getValue());
            }
        }

        Map<String, Double> rankedTopics = selectionManager.getRankedTopics(selectionMethod, selectResults);

        Map<String, Double> topics = new LinkedHashMap<>();
        // rankS has its own limit mechanism
        if (RankS.class.getSimpleName().equals(methodName)) {
            for (Map.Entry<String, Double> entry : rankedTopics.entrySet()) {
                if (entry.getValue() < ranks_min)
                    break;
                topics.put(entry.getKey(), entry.getValue());
            }
        } else { // hard limit
            int i = 0;
            for (Map.Entry<String, Double> entry : rankedTopics.entrySet()) {
                i++;
                if (i > col_max)
                    break;
                topics.put(entry.getKey(), entry.getValue());
            }
        }

        if (topics.size() > 0) {
            selectResults = selectionManager.filterTopics(topics.keySet(), selectResults);


            FeatureVector queryFV = new FeatureVector(null);
            Iterator<String> qTerms = AnalyzerUtils.analyze(new StopperTweetAnalyzer(Version.LUCENE_43, false), query).iterator();
            while (qTerms.hasNext()) {
                String term = qTerms.next();
                if ("AND".equals(term) || "OR".equals(term))
                    continue;
                queryFV.addTerm(term.toLowerCase(), 1.0);
            }
            queryFV.normalizeToOne();

            // cap results
            List<Document> results = selectResults.subList(0, Math.min(50, selectResults.size()));

            FeedbackRelevanceModel fb = new FeedbackRelevanceModel();
            fb.setOriginalQueryFV(queryFV);
            fb.setRes(results);
            fb.build(null);

            FeatureVector fbVector = fb.asFeatureVector();
            fbVector.pruneToSize(20);
            fbVector.normalizeToOne();
            fbVector = FeatureVector.interpolate(queryFV, fbVector, 0.5); // ORIG_QUERY_WEIGHT

            logger.info("Feature Vector for topics {}:\n{}", Joiner.on(", ").join(topics.keySet()), fbVector.toString());

            StringBuilder builder = new StringBuilder();
            Iterator<String> terms = fbVector.iterator();
            while (terms.hasNext()) {
                String term = terms.next();
                double prob = fbVector.getFeaturetWeight(term);
                if (prob < 0)
                    continue;
                builder.append('"').append(term).append('"').append("^").append(prob).append(" ");
            }
            query = builder.toString().trim();
        }

        List<Document> searchResults;

        if (max_id.isPresent()) {
            searchResults = searchManager.search(query, n, filterRT, maxId);
        } else if (epoch_range.isPresent()) {
            long firstEpoch = 0L;
            long lastEpoch = Long.MAX_VALUE;
            String[] epochs = epoch_range.get().split("[: ]");
            try {
                if (epochs.length == 1) {
                    lastEpoch = Long.parseLong(epochs[0]);
                } else {
                    firstEpoch = Long.parseLong(epochs[0]);
                    lastEpoch = Long.parseLong(epochs[1]);
                }
            } catch (Exception e) {
                // pass
            }
            searchResults = searchManager.search(query, n, filterRT, firstEpoch, lastEpoch);
        } else {
            searchResults = searchManager.search(query, n, filterRT);
        }

        int totalHits = searchResults != null ? searchResults.size() : 0;

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectDocumentsResponse documentsResponse = new SelectDocumentsResponse(sources, topics, methodName, totalHits, 0, selectResults, searchResults);
        return new SelectSearchResponse(responseHeader, documentsResponse);
    }
}
