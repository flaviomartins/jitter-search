package io.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.caching.CacheControl;
import io.dropwizard.jersey.params.DateTimeParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.RMTSDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.rerank.*;
import io.jitter.core.search.SearchManager;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.utils.Epochs;
import io.swagger.annotations.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/rmts")
@Api(value = "/rmts", description = "Enhanced temporal search endpoint")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class RMTSResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(RMTSResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;
    private final TailyManager tailyManager;

    public RMTSResource(SearchManager searchManager, SelectionManager selectionManager, ShardsManager shardsManager, TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(searchManager);
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
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
            @ApiResponse(code = 404, message = "No results found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public SelectionSearchResponse search(@ApiParam(value = "Search query", required = true) @QueryParam("q") @NotEmpty String q,
                                          @ApiParam(hidden = true) @QueryParam("fq") Optional<String> fq,
                                          @ApiParam(value = "Limit results", allowableValues="range[1, 10000]") @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                          @ApiParam(value = "Include retweets") @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                          @ApiParam(value = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                          @ApiParam(value = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                          @ApiParam(value = "Day filter") @QueryParam("day") Optional<DateTimeParam> day,
                                          @ApiParam(value = "Limit feedback results", allowableValues="range[1, 10000]") @QueryParam("sLimit") @DefaultValue("1000") Integer sLimit,
                                          @ApiParam(value = "Include retweets for feedback") @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                          @ApiParam(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                          @ApiParam(value = "Resource selection method", allowableValues="taily,ranks,crcsexp,crcslin,votes,sizes") @QueryParam("method") @DefaultValue("ranks") String method,
                                          @ApiParam(value = "Maximum number of collections", allowableValues="range[0, 100]") @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                          @ApiParam(value = "Rank-S parameter", allowableValues="range[0, 1]") @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @ApiParam(value = "Use collection size normalization") @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                          @ApiParam(value = "Taily parameter", allowableValues="range[0, 100]") @QueryParam("v") @DefaultValue("10") Integer v,
                                          @ApiParam(value = "Force topic") @QueryParam("topic") Optional<String> topic,
                                          @ApiParam(value = "Number of feedback documents", allowableValues="range[1, 1000]") @QueryParam("fbDocs") @DefaultValue("50") Integer fbDocs,
                                          @ApiParam(value = "Number of feedback terms", allowableValues="range[1, 1000]") @QueryParam("fbTerms") @DefaultValue("20") Integer fbTerms,
                                          @ApiParam(value = "Original query weight", allowableValues="range[0, 1]") @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                          @ApiParam(value = "Number of feedback collections") @QueryParam("fbCols") @DefaultValue("3") Integer fbCols,
                                          @ApiParam(value = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                          @ApiParam(hidden = true) @QueryParam("rerank") @DefaultValue("true") Boolean rerank,
                                          @ApiParam(value = "Number of documents to rerank", allowableValues="range[1, 1000]") @QueryParam("numRerank") @DefaultValue("1000") Integer numRerank,
                                          @ApiParam(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q, "UTF-8");
            String filterQuery = URLDecoder.decode(fq.orElse(""), "UTF-8");
            long[] epochs = Epochs.parseEpoch(epoch);

            if (day.isPresent()) {
                DateTimeParam dateTimeParam = day.get();
                epochs = Epochs.parseDay(dateTimeParam.get());
            }

            Selection selection;
            if ("taily".equalsIgnoreCase(method)) {
                selection = tailyManager.selection(query, v, topics);
            } else {
                selection = selectionManager.selection(query, filterQuery, maxId, epochs, sLimit, sRetweets, sFuture,
                        method, maxCol, minRanks, normalize, topics);
            }

            Set<String> selected;
            if (topic.isPresent()) {
                selected = Sets.newHashSet(topic.get());
            } else {
                selected = Sets.newHashSet(Iterables.limit(selection.getCollections().keySet(), fbCols));
            }

            SelectionTopDocuments shardResults = shardsManager.search(maxId, epochs, sRetweets, sFuture, fbDocs, topics, query, filterQuery, selected);

            // get the query epoch
            double currentEpoch = System.currentTimeMillis() / 1000L;
            double queryEpoch = epoch.isPresent() ? epochs[1] : currentEpoch;

            TopDocuments results = searchManager.search(query, filterQuery, maxId, limit, retweets, epochs);

            RerankerCascade cascade = new RerankerCascade();
            cascade.add(new RMTSReranker("rmts.model", query, queryEpoch, (List<StatusDocument>) shardResults.scoreDocs, searchManager.getAnalyzer(), searchManager.getCollectionStats(), limit, numRerank, rerank));
            cascade.add(new MeanTFFilter(3));

            RerankerContext context = new RerankerContext(null, null, "MB000", query,
                    queryEpoch, Lists.newArrayList(), IndexStatuses.StatusField.TEXT.name, null);
            results.scoreDocs = cascade.run((List<StatusDocument>) results.scoreDocs, context);

            int totalHits = results.totalHits;
            if (totalHits == 0) {
                throw new NotFoundException("No results found");
            }

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            RMTSDocumentsResponse documentsResponse = new RMTSDocumentsResponse(selection.getCollections().entrySet(), method, 0, selection.getResults(), shardResults, results);
            return new SelectionSearchResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
