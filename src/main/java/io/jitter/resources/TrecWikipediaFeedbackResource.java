package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.FeedbackDocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import io.jitter.core.wikipedia.WikipediaManager;
import io.jitter.core.wikipedia.WikipediaTopDocuments;
import io.swagger.annotations.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
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

@Path("/trec/wikipedia/fb")
@Api(value = "/trec/wikipedia/fb", description = "TREC Feedback search endpoint")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecWikipediaFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecWikipediaFeedbackResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final WikipediaManager wikipediaManager;

    public TrecWikipediaFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, WikipediaManager wikipediaManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(wikipediaManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.wikipediaManager = wikipediaManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    @ApiOperation(
            value = "Searches documents by keyword query using feedback",
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
                                 @ApiParam(value = "Limit feedback results", allowableValues="range[1, 10000]") @QueryParam("sLimit") @DefaultValue("1000") Integer sLimit,
                                 @ApiParam(value = "Include retweets for feedback") @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                 @ApiParam(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                 @ApiParam(value = "Number of feedback documents", allowableValues="range[1, 1000]") @QueryParam("fbDocs") @DefaultValue("50") Integer fbDocs,
                                 @ApiParam(value = "Number of feedback terms", allowableValues="range[1, 1000]") @QueryParam("fbTerms") @DefaultValue("20") Integer fbTerms,
                                 @ApiParam(value = "Original query weight", allowableValues="range[0, 1]") @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                 @ApiParam(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        if (!q.isPresent() || q.get().isEmpty()) {
            throw new BadRequestException();
        }

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q.orElse(""), "UTF-8");

            WikipediaTopDocuments selectResults = wikipediaManager.search(query, limit, true);

            String finalQuery = query;
            FeatureVector fbVector = null;
            if (fbDocs > 0 && fbTerms > 0) {
                FeatureVector queryFV = buildQueryFV(query, wikipediaManager.getStopper());
                FeatureVector feedbackFV = buildFeedbackFV(fbDocs, fbTerms, selectResults.scoreDocs, trecMicroblogAPIWrapper.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());
                fbVector = interpruneFV(fbTerms, fbWeight.floatValue(), queryFV, feedbackFV);
                finalQuery = buildQuery(fbVector);
            }

            TopDocuments results = trecMicroblogAPIWrapper.search(finalQuery, maxId, limit, retweets);

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
        } catch (IOException | TException | ClassNotFoundException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}