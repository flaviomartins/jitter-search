package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.dropwizard.jersey.jsr310.LocalDateTimeParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.utils.Epochs;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/select")
@Tag(name = "/select", description = "Resource selection")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectionResource {
    private static final Logger logger = LoggerFactory.getLogger(SelectionResource.class);

    private final AtomicLong counter;
    private final SelectionManager selectionManager;
    private final TailyManager tailyManager;

    public SelectionResource(SelectionManager selectionManager, TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.selectionManager = selectionManager;
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
            @ApiResponse(responseCode = "404", description = "No results found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    public SelectionResponse search(@Parameter(name = "Search query", required = true) @QueryParam("q") @NotBlank String q,
                                    @Parameter(hidden = true) @QueryParam("fq") Optional<String> fq,
                                    @Parameter(name = "Limit results", schema = @Schema(minimum = "1", maximum = "1000")) @QueryParam("limit") @DefaultValue("50") Integer limit,
                                    @Parameter(name = "Include retweets") @QueryParam("retweets") @DefaultValue("true") Boolean retweets,
                                    @Parameter(name = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                    @Parameter(name = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                    @Parameter(name = "Day filter") @QueryParam("day") Optional<LocalDateTimeParam> day,
                                    @Parameter(name = "Limit feedback results", schema = @Schema(minimum = "1", maximum = "10000")) @QueryParam("sLimit") @DefaultValue("1000") Integer sLimit,
                                    @Parameter(name = "Include retweets for feedback") @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                    @Parameter(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                    @Parameter(name = "Resource selection method", schema = @Schema(allowableValues = {"taily", "ranks", "crcsexp", "crcslin", "votes", "sizes"})) @QueryParam("method") @DefaultValue("ranks") String method,
                                    @Parameter(name = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                    @Parameter(name = "Maximum number of collections", schema = @Schema(minimum = "1", maximum = "100")) @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                    @Parameter(name = "Rank-S parameter", schema = @Schema(minimum = "1", maximum = "1")) @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                    @Parameter(name = "Use collection size normalization") @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                    @Parameter(name = "Taily parameter", schema = @Schema(minimum = "1", maximum = "100")) @QueryParam("v") @DefaultValue("10") Integer v,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q, StandardCharsets.UTF_8);
            String filterQuery = URLDecoder.decode(fq.orElse(""), StandardCharsets.UTF_8);
            long[] epochs = Epochs.parseEpoch(epoch);

            if (day.isPresent()) {
                LocalDateTimeParam dateTimeParam = day.get();
                epochs = Epochs.parseDay(dateTimeParam.get());
            }

            int totalHits = 0;
            int c_sel;
            Selection selection;
            if ("taily".equalsIgnoreCase(method)) {
                selection = tailyManager.selection(query, v, topics);
                if (topics) {
                    c_sel = tailyManager.getTopics().size();
                } else {
                    c_sel = tailyManager.getUsers().size();
                }
            } else {
                selection = selectionManager.selection(query, filterQuery, maxId, epochs, sLimit, sRetweets, sFuture,
                        method, maxCol, minRanks, normalize, topics);
                SelectionTopDocuments selectionTopDocuments = selection.getResults();
                c_sel = selectionTopDocuments.getC_sel();
                totalHits = selectionTopDocuments.totalHits;
            }

            Map<String, Double> selected = selection.getCollections();

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(selected.entrySet(), method, c_sel, totalHits, 0);
            return new SelectionResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
