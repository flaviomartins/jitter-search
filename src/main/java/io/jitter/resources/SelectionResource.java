package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.Document;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.methods.RankS;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import org.apache.lucene.queryparser.classic.ParseException;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/select")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectionResource {
    private static final Logger logger = LoggerFactory.getLogger(SelectionResource.class);

    private final AtomicLong counter;
    private final SelectionManager selectionManager;

    public SelectionResource(SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.selectionManager = selectionManager;
    }

    @GET
    @Timed
    public SelectionResponse search(@QueryParam("q") Optional<String> q,
                                    @QueryParam("fq") Optional<String> filterQuery,
                                    @QueryParam("limit") Optional<Integer> limit,
                                    @QueryParam("max_id") Optional<Long> max_id,
                                    @QueryParam("epoch") Optional<String> epoch_range,
                                    @QueryParam("filter_rt") Optional<Boolean> filter_rt,
                                    @QueryParam("method") Optional<String> method,
                                    @QueryParam("topics") Optional<Boolean> topics,
                                    @QueryParam("max_col") Optional<Integer> max_col,
                                    @QueryParam("min_ranks") Optional<Double> min_ranks,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        int n = limit.or(50);
        long maxId = max_id.or(-1L);
        boolean filterRT = filter_rt.or(false);

        String methodText = method.or(selectionManager.getMethod());
        boolean isTopics = topics.or(false);
        int col_max = max_col.or(3);
        double ranks_min = min_ranks.or(1e-5);

        long startTime = System.currentTimeMillis();

        List<Document> selectResults;

        if (max_id.isPresent()) {
            selectResults = selectionManager.search(query, n, filterRT, maxId);
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
            selectResults = selectionManager.search(query, n, filterRT, firstEpoch, lastEpoch);
        } else {
            selectResults = selectionManager.search(query, n, filterRT);
        }


        int totalHits = selectResults != null ? selectResults.size() : 0;

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(methodText);
        String methodName = selectionMethod.getClass().getSimpleName();

        Map<String, Double> ranking;
        if (isTopics) {
            ranking = selectionManager.getRankedTopics(selectionMethod, selectResults);
        } else {
            ranking = selectionManager.getRanked(selectionMethod, selectResults);
        }

        Map<String, Double> map = new LinkedHashMap<>();
        // rankS has its own limit mechanism
        if (RankS.class.getSimpleName().equals(methodName)) {
            for (Map.Entry<String, Double> entry : ranking.entrySet()) {
                if (entry.getValue() < ranks_min)
                    break;
                map.put(entry.getKey(), entry.getValue());
            }
        } else { // hard limit
            int i = 0;
            for (Map.Entry<String, Double> entry : ranking.entrySet()) {
                i++;
                if (i > col_max)
                    break;
                map.put(entry.getKey(), entry.getValue());
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(map, methodName, totalHits, 0, selectResults);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
