package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.DocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.search.SearchManager;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.Epochs;
import io.swagger.annotations.*;
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
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/search")
@Api(value = "/search", description = "Search endpoint")
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
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    @ApiOperation(
            value = "Searches documents by keyword query",
            notes = "Returns a search response",
            response = SearchResponse.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid query supplied"),
            @ApiResponse(code = 404, message = "No results found")
    })
    public SearchResponse search(@ApiParam(value = "Search query", required = true) @QueryParam("q") Optional<String> q,
                                 @ApiParam(hidden = true) @QueryParam("fq") Optional<String> fq,
                                 @ApiParam(value = "Results limit") @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                 @ApiParam(hidden = true) @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                 @ApiParam(hidden = true) @QueryParam("maxId") Optional<Long> maxId,
                                 @ApiParam(hidden = true) @QueryParam("epoch") Optional<String> epoch,
                                 @ApiParam(hidden = true) @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);

        long startTime = System.currentTimeMillis();

        TopDocuments results = searchManager.search(limit.get(), retweets.get(), maxId, epoch, query, epochs);

        long endTime = System.currentTimeMillis();

        int totalHits = results != null ? results.totalHits : 0;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        DocumentsResponse documentsResponse = new DocumentsResponse(totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
