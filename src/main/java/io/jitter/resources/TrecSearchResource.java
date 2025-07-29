package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.DocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trec/search")
@Tag(name = "/trec/search", description = "TREC Search endpoint")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecSearchResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecSearchResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;

    public TrecSearchResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    @Operation(
            summary = "Searches documents by keyword query",
            description = "Returns a search response"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid query"),
            @ApiResponse(responseCode = "404", description = "No results found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    public SearchResponse search(@Parameter(name = "Search query", required = true) @QueryParam("q") @NotBlank String q,
                                 @Parameter(hidden = true) @QueryParam("fq") Optional<String> fq,
                                 @Parameter(name = "Limit results", schema = @Schema(minimum = "1", maximum = "10000")) @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                 @Parameter(name = "Include retweets") @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                 @Parameter(name = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                 @Parameter(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q, StandardCharsets.UTF_8);

            TopDocuments results = trecMicroblogAPIWrapper.search(query, maxId, limit, retweets);
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
        } catch (IOException | TException | ClassNotFoundException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
