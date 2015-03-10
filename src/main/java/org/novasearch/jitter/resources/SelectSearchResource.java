package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.search.*;
import org.novasearch.jitter.core.search.SearchManager;
import org.novasearch.jitter.core.selection.SelectionManager;
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;

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
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;

@Path("/ss")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectSearchResource {
    private static final Logger logger = Logger.getLogger(SelectSearchResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;
    private SelectionManager selectionManager;

    public SelectSearchResource(SearchManager searchManager, SelectionManager selectionManager) throws IOException {
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
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        int n = limit.or(30);
        long maxId = max_id.or(-1L);
        boolean filterRT = filter_rt.or(false);
        String methodText = method.or(selectionManager.getMethod());
        int s = slimit.or(50);

        long startTime = System.currentTimeMillis();

        List<Document> selectResults = selectionManager.search(query, s);

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(methodText);
        String methodName = selectionMethod.getClass().getSimpleName();

        SortedMap<String, Float> ranked = selectionManager.getRankedTopics(selectionMethod, selectResults);

        List<Document> searchResults;

        if (max_id.isPresent()) {
            searchResults = searchManager.search(query, n, filterRT, maxId);
        } else if (epoch_range.isPresent()) {
            long firstEpoch = 0L;
            long lastEpoch = Long.MAX_VALUE;
            String[] epochs = epoch_range.get().split("[: ]");
            try {
                firstEpoch = Long.parseLong(epochs[0]);
                lastEpoch = Long.parseLong(epochs[1]);
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
        SelectDocumentsResponse documentsResponse = new SelectDocumentsResponse(ranked, methodName, totalHits, 0, searchResults);
        return new SelectSearchResponse(responseHeader, documentsResponse);
    }
}
