package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.taily.TailyManager;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/taily")
@Tag(name = "/taily", description = "Vocabulary-based resource selection")
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
    @Operation(
            summary = "Searches documents by keyword query using a time-aware ranking model",
            description = "Returns a selection search response"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid query"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    public SelectionResponse search(@Parameter(name = "Search query", required = true) @QueryParam("q") @NotBlank String q,
                                    @Parameter(name = "Taily parameter", schema = @Schema(minimum = "1", maximum = "100")) @QueryParam("v") @DefaultValue("10") Integer v,
                                    @Parameter(name = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                    @Parameter(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        long startTime = System.currentTimeMillis();
        String query = URLDecoder.decode(q, StandardCharsets.UTF_8);

        int c_sel;
        Map<String, Double> ranking = tailyManager.select(query, v, topics);
        if (topics) {
            c_sel = tailyManager.getTopics().size();
        } else {
            c_sel = tailyManager.getUsers().size();
        }

        long endTime = System.currentTimeMillis();
        logger.info(String.format(Locale.ENGLISH, "%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(ranking.entrySet(), "Taily", c_sel, 0, 0);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
