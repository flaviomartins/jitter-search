package io.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.search.SelectionFeedbackDocumentsResponse;
import io.jitter.core.rerank.MaxTFFilter;
import io.jitter.core.rerank.RerankerCascade;
import io.jitter.core.rerank.RerankerContext;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.utils.Epochs;
import io.swagger.annotations.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trec/shardsfb")
@Api(value = "/trec/shardsfb", description = "TREC Shards Feedback search endpoint")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecShardsFeedbackResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecShardsFeedbackResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;
    private final ShardsManager shardsManager;

    public TrecShardsFeedbackResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper, ShardsManager shardsManager) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);
        Preconditions.checkNotNull(shardsManager);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
        this.shardsManager = shardsManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    @ApiOperation(
            value = "Searches documents by keyword query using vertical feedback",
            notes = "Returns a selection search response",
            response = SelectionSearchResponse.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid query"),
            @ApiResponse(code = 404, message = "No results found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public SelectionSearchResponse search(@ApiParam(value = "Search query", required = true) @QueryParam("q") Optional<String> q,
                                          @ApiParam(hidden = true) @QueryParam("fq") Optional<String> fq,
                                          @ApiParam(value = "Limit results", allowableValues="range[1, 10000]") @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                          @ApiParam(value = "Include retweets") @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                          @ApiParam(value = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                          @ApiParam(value = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                          @ApiParam(value = "Limit feedback results", allowableValues="range[1, 10000]") @QueryParam("sLimit") @DefaultValue("1000") Integer sLimit,
                                          @ApiParam(value = "Include retweets for feedback") @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                          @ApiParam(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                          @ApiParam(value = "Resource selection method", allowableValues="taily,ranks,crcsexp,crcslin,votes,sizes") @QueryParam("method") @DefaultValue("crcsexp") String method,
                                          @ApiParam(value = "Maximum number of collections", allowableValues="range[0, 100]") @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                          @ApiParam(value = "Rank-S parameter", allowableValues="range[0, 1]") @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @ApiParam(value = "Use collection size normalization") @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                          @ApiParam(value = "Taily parameter", allowableValues="range[0, 100]") @QueryParam("v") @DefaultValue("10") Integer v,
                                          @ApiParam(value = "Force topic") @QueryParam("topic") Optional<String> topic,
                                          @ApiParam(value = "Number of feedback documents", allowableValues="range[1, 1000]") @QueryParam("fbDocs") @DefaultValue("50") Integer fbDocs,
                                          @ApiParam(value = "Number of feedback terms", allowableValues="range[1, 1000]") @QueryParam("fbTerms") @DefaultValue("20") Integer fbTerms,
                                          @ApiParam(value = "Original query weight", allowableValues="range[0, 1]") @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                          @ApiParam(value = "Number of feedback collections") @QueryParam("fbCols") @DefaultValue("3") Integer fbCols,
                                          @ApiParam(hidden = true) @QueryParam("fbMerge") @DefaultValue("false") Boolean fbMerge,
                                          @ApiParam(value = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                          @ApiParam(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        if (!q.isPresent() || q.get().isEmpty()) {
            throw new BadRequestException();
        }

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q.orElse(""), "UTF-8");
            long[] epochs = Epochs.parseEpoch(epoch);

            SelectionTopDocuments shardResults = shardsManager.search(maxId, epoch, sRetweets, sFuture, limit, topics, query, epochs, null);
            shardResults.scoreDocs = shardResults.scoreDocs.subList(0, Math.min(fbDocs, shardResults.scoreDocs.size()));

            FeatureVector shardsFV = null;
            if (shardResults.totalHits > 0) {
                shardsFV = buildFeedbackFV(fbDocs, fbTerms, shardResults.scoreDocs, shardsManager.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());
            }

            FeatureVector feedbackFV = null;
            FeatureVector fbVector;
            if (fbMerge) {
                TopDocuments selectResults = trecMicroblogAPIWrapper.search(query, maxId, limit, retweets);
                feedbackFV = buildFeedbackFV(fbDocs, fbTerms, selectResults.scoreDocs, trecMicroblogAPIWrapper.getStopper(), trecMicroblogAPIWrapper.getCollectionStats());
                fbVector = interpruneFV(fbTerms, fbWeight.floatValue(), shardsFV, feedbackFV);
            } else {
                fbVector = shardsFV;
            }

            FeatureVector queryFV = buildQueryFV(query, trecMicroblogAPIWrapper.getStopper());
            fbVector = interpruneFV(fbTerms, fbWeight.floatValue(), queryFV, fbVector);
            String finalQuery = buildQuery(fbVector);

            // get the query epoch
            double currentEpoch = System.currentTimeMillis() / 1000L;
            double queryEpoch = epoch.isPresent() ? epochs[1] : currentEpoch;

            TopDocuments results = trecMicroblogAPIWrapper.search(finalQuery, maxId, limit, retweets);

            RerankerCascade cascade = new RerankerCascade();
            cascade.add(new MaxTFFilter(5));

            RerankerContext context = new RerankerContext(null, null, "MB000", query,
                    queryEpoch, Lists.newArrayList(), IndexStatuses.StatusField.TEXT.name, null);
            results.scoreDocs = cascade.run(results.scoreDocs, context);

            int totalFbDocs = shardResults.totalHits;
            int totalHits = results.totalHits;
            if (totalHits == 0) {
                throw new NotFoundException("No results found");
            }

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            SelectionFeedbackDocumentsResponse documentsResponse = new SelectionFeedbackDocumentsResponse(null, null, method, totalFbDocs, fbTerms, shardsFV.getMap(), feedbackFV != null ? feedbackFV.getMap() : null, fbVector.getMap(), 0, null, shardResults, results);
            return new SelectionSearchResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException | TException | ClassNotFoundException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
