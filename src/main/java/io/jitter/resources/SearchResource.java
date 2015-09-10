package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.Document;
import io.jitter.api.search.DocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.search.SearchManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
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
                                 @QueryParam("fq") Optional<String> fq,
                                 @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                 @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                 @QueryParam("maxId") Optional<Long> maxId,
                                 @QueryParam("epoch") Optional<String> epoch,
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or("empty"), "UTF-8");
        List<Document> results = null;

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                results = searchManager.search(query, limit.get(), !retweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                results = searchManager.search(query, limit.get(), !retweets.get(), epochs[0], epochs[1]);
            } else {
                results = searchManager.search(query, limit.get(), !retweets.get());
            }
        }

        long endTime = System.currentTimeMillis();

        int totalHits = results != null ? results.size() : 0;

        logger.info(String.format("%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        DocumentsResponse documentsResponse = new DocumentsResponse(totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
