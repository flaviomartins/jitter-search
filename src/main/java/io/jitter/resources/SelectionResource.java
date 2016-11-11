package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/select")
@Api(value = "/select", description = "Collection-based resource selection")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectionResource {
    private static final Logger logger = LoggerFactory.getLogger(SelectionResource.class);

    private final AtomicLong counter;
    private final SelectionManager selectionManager;

    public SelectionResource(SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.selectionManager = selectionManager;
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
            @ApiResponse(code = 404, message = "No results found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public SelectionResponse search(@ApiParam(value = "Search query", required = true) @QueryParam("q") Optional<String> q,
                                    @ApiParam(hidden = true) @QueryParam("fq") Optional<String> fq,
                                    @ApiParam(value = "Limit results", allowableValues="range[1, 1000]") @QueryParam("limit") @DefaultValue("50") Integer limit,
                                    @ApiParam(value = "Include retweets") @QueryParam("retweets") @DefaultValue("true") Boolean retweets,
                                    @ApiParam(value = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                    @ApiParam(value = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                    @ApiParam(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean future,
                                    @ApiParam(value = "Resource selection method", allowableValues="taily,ranks,crcsexp,crcslin,votes,sizes") @QueryParam("method") @DefaultValue("crcsexp") String method,
                                    @ApiParam(value = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                    @ApiParam(value = "Maximum number of collections", allowableValues="range[0, 100]") @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                    @ApiParam(value = "Rank-S parameter", allowableValues="range[0, 1]") @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                    @ApiParam(value = "Use collection size normalization") @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        if (!q.isPresent() || q.get().isEmpty()) {
            throw new BadRequestException();
        }

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q.orElse(""), "UTF-8");
            long[] epochs = Epochs.parseEpoch(epoch);

            SelectionTopDocuments selectResults = selectionManager.search(maxId, epoch, limit, retweets, future, query, epochs);
            SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);

            Map<String, Double> selected = selectionManager.select(limit, topics, maxCol, minRanks, normalize, selectResults, selectionMethod);

            int totalHits = selectResults.totalHits;
            if (totalHits == 0) {
                throw new NotFoundException("No results found");
            }

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(selected, method, 0, selectResults);
            return new SelectionResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
