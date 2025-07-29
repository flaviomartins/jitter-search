package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.dropwizard.jersey.jsr310.LocalDateTimeParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.FeedbackDocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.search.SearchManager;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.Epochs;
import io.jitter.core.wikipedia.WikipediaManager;
import io.jitter.core.wikipedia.WikipediaTopDocuments;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/wikipedia/fb")
@Tag(name = "/wikipedia/fb", description = "Wikipedia feedback search endpoint")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class WikipediaFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(WikipediaFeedbackResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;
    private final WikipediaManager wikipediaManager;

    public WikipediaFeedbackResource(SearchManager searchManager, WikipediaManager wikipediaManager) throws IOException {
        Preconditions.checkNotNull(searchManager);
        Preconditions.checkNotNull(wikipediaManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
        this.wikipediaManager = wikipediaManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    @Operation(
            summary = "Searches documents by keyword query using feedback",
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
                                 @Parameter(name = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                 @Parameter(name = "Day filter") @QueryParam("day") Optional<LocalDateTimeParam> day,
                                 @Parameter(name = "Limit feedback results", schema = @Schema(minimum = "1", maximum = "10000")) @QueryParam("sLimit") @DefaultValue("1000") Integer sLimit,
                                 @Parameter(name = "Include retweets for feedback") @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                 @Parameter(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                 @Parameter(name = "Number of feedback documents", schema = @Schema(minimum = "1", maximum = "1000")) @QueryParam("fbDocs") @DefaultValue("50") Integer fbDocs,
                                 @Parameter(name = "Number of feedback terms", schema = @Schema(minimum = "1", maximum = "1000")) @QueryParam("fbTerms") @DefaultValue("20") Integer fbTerms,
                                 @Parameter(name = "Original query weight", schema = @Schema(minimum = "1", maximum = "1")) @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                 @Parameter(hidden = true) @Context UriInfo uriInfo) {
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

            WikipediaTopDocuments selectResults = wikipediaManager.search(query, "", limit, true);

            String finalQuery = query;
            FeatureVector fbVector = null;
            if (fbDocs > 0 && fbTerms > 0) {
                FeatureVector queryFV = buildQueryFV(query, wikipediaManager.getStopper());
                FeatureVector feedbackFV = buildFeedbackFV(fbDocs, fbTerms, selectResults.scoreDocs, searchManager.getStopper(), searchManager.getCollectionStats());
                fbVector = interpruneFV(fbTerms, fbWeight.floatValue(), queryFV, feedbackFV);
                finalQuery = buildQuery(fbVector);
            }

            TopDocuments results = searchManager.search(finalQuery, filterQuery, maxId, limit, retweets, epochs);

            int totalFbDocs = selectResults != null ? selectResults.scoreDocs.size() : 0;
            int totalHits = results != null ? results.totalHits : 0;
            if (totalHits == 0) {
                throw new NotFoundException("No results found");
            }

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dfbDocs %4dhits %s", (endTime - startTime), totalFbDocs, totalHits, finalQuery));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            FeedbackDocumentsResponse documentsResponse = new FeedbackDocumentsResponse(totalFbDocs, fbTerms, fbVector.getMap(), 0, results);
            return new SearchResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
