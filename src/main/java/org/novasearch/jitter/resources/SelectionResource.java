package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.lucene.queryparser.classic.ParseException;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.selection.SelectionDocumentsResponse;
import org.novasearch.jitter.api.selection.SelectionResponse;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.core.selection.SelectionManager;
import org.novasearch.jitter.core.selection.methods.RankS;
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;
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
                                    @QueryParam("method") Optional<String> method,
                                    @QueryParam("limit") Optional<Integer> limit,
                                    @QueryParam("max_col") Optional<Integer> max_col,
                                    @QueryParam("min_ranks") Optional<Double> min_ranks,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        String methodText = method.or(selectionManager.getMethod());
        int n = limit.or(50);
        int col_max = max_col.or(3);
        double ranks_min = min_ranks.or(1e-5);

        long startTime = System.currentTimeMillis();

        List<Document> results = selectionManager.search(query, n);
        int totalHits = results != null ? results.size() : 0;

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(methodText);
        String methodName = selectionMethod.getClass().getSimpleName();

        Map<String, Double> ranking = selectionManager.getRanked(selectionMethod, results);

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
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(map, methodName, totalHits, 0, results);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
