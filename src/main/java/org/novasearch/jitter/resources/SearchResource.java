package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.lucene.queryparser.classic.ParseException;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.api.search.DocumentsResponse;
import org.novasearch.jitter.api.search.SearchResponse;
import org.novasearch.jitter.core.search.SearchManager;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Path("/search")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SearchResource {
    private static final Logger logger = LoggerFactory.getLogger(SearchResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;

    public SearchResource(SearchManager searchManager) throws IOException {
        Preconditions.checkNotNull(searchManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
    }

    @GET
    @Timed
    public SearchResponse search(@QueryParam("q") Optional<String> q,
                                 @QueryParam("fq") Optional<String> filterQuery,
                                 @QueryParam("limit") Optional<Integer> limit,
                                 @QueryParam("max_id") Optional<Long> max_id,
                                 @QueryParam("epoch") Optional<String> epoch_range,
                                 @QueryParam("filter_rt") Optional<Boolean> filter_rt,
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        int n = limit.or(1000);
        long maxId = max_id.or(-1L);
        boolean filterRT = filter_rt.or(false);

        long startTime = System.currentTimeMillis();

        List<Document> results;

        if (max_id.isPresent()) {
            results = searchManager.search(query, n, filterRT, maxId);
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
            results = searchManager.search(query, n, filterRT, firstEpoch, lastEpoch);
        } else {
            results = searchManager.search(query, n, filterRT);
        }

        int totalHits = results != null ? results.size() : 0;

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        DocumentsResponse documentsResponse = new DocumentsResponse(totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
