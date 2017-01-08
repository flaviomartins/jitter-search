package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
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
import javax.ws.rs.core.*;
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
            @ApiResponse(code = 400, message = "Invalid query"),
            @ApiResponse(code = 404, message = "No results found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public SearchResponse search(@ApiParam(value = "Search query", required = true) @QueryParam("q") Optional<String> q,
                                 @ApiParam(hidden = true) @QueryParam("fq") Optional<String> fq,
                                 @ApiParam(value = "Limit results", allowableValues="range[1, 10000]") @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                 @ApiParam(value = "Include retweets") @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                 @ApiParam(value = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                 @ApiParam(value = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                 @ApiParam(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        if (!q.isPresent() || q.get().isEmpty()) {
            throw new BadRequestException();
        }

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q.orElse(""), "UTF-8");
            long[] epochs = Epochs.parseEpoch(epoch);

            TopDocuments results = searchManager.search(query, maxId, limit, retweets, epochs);
            int totalHits = results != null ? results.totalHits : 0;
            if (totalHits == 0) {
                throw new NotFoundException("No results found");
            }

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            DocumentsResponse documentsResponse = new DocumentsResponse(totalHits, 0, results);
            return new SearchResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
