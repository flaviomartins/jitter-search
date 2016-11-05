package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.taily.TailyManager;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/taily")
@Api(value = "/taily", description = "Vocabulary-based resource selection")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TailyResource {
    private static final Logger logger = LoggerFactory.getLogger(TailyResource.class);

    private final AtomicLong counter;
    private final TailyManager tailyManager;

    public TailyResource(TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.tailyManager = tailyManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    @ApiOperation(
            value = "Searches documents by keyword query using a time-aware ranking model",
            notes = "Returns a selection search response",
            response = SelectionSearchResponse.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid query"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public SelectionResponse search(@ApiParam(value = "Search query", required = true) @QueryParam("q") Optional<String> q,
                                    @ApiParam(value = "Taily parameter", allowableValues="range[0, 100]") @QueryParam("v") @DefaultValue("10") Integer v,
                                    @ApiParam(value = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                    @ApiParam(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        if (!q.isPresent() || q.get().isEmpty()) {
            throw new BadRequestException();
        }

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q.orElse(""), "UTF-8");

            Map<String, Double> ranking = tailyManager.select(query, v, topics);

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %s", (endTime - startTime), query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(ranking, "Taily", 0, 0, 0);
            return new SelectionResponse(responseHeader, documentsResponse);
        } catch (IOException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
