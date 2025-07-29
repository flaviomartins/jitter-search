package io.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionFeedbackDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.rerank.RMTSReranker;
import io.jitter.core.rerank.RerankerCascade;
import io.jitter.core.rerank.RerankerContext;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trec/shardsfb")
@Tag(name = "/trec/shardsfb", description = "TREC Shards Feedback search endpoint")
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
    @Operation(
            summary = "Searches documents by keyword query using vertical feedback",
            description = "Returns a selection search response"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "Invalid query"),
            @ApiResponse(responseCode = "404", description = "No results found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    public SelectionSearchResponse search(@Parameter(name = "Search query", required = true) @QueryParam("q") @NotBlank String q,
                                          @Parameter(hidden = true) @QueryParam("fq") Optional<String> fq,
                                          @Parameter(name = "Limit results", schema = @Schema(minimum = "1", maximum = "10000")) @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                          @Parameter(name = "Include retweets") @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                          @Parameter(name = "Maximum document id") @QueryParam("maxId") Optional<Long> maxId,
                                          @Parameter(name = "Epoch filter") @QueryParam("epoch") Optional<String> epoch,
                                          @Parameter(name = "Limit feedback results", schema = @Schema(minimum = "1", maximum = "10000")) @QueryParam("sLimit") @DefaultValue("1000") Integer sLimit,
                                          @Parameter(name = "Include retweets for feedback") @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                          @Parameter(hidden = true) @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                          @Parameter(name = "Resource selection method", schema = @Schema(allowableValues = {"taily", "ranks", "crcsexp", "crcslin", "votes", "sizes"})) @QueryParam("method") @DefaultValue("ranks") String method,
                                          @Parameter(name = "Maximum number of collections", schema = @Schema(minimum = "1", maximum = "100")) @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                          @Parameter(name = "Rank-S parameter", schema = @Schema(minimum = "1", maximum = "1")) @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @Parameter(name = "Use collection size normalization") @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                          @Parameter(name = "Taily parameter", schema = @Schema(minimum = "1", maximum = "100")) @QueryParam("v") @DefaultValue("10") Integer v,
                                          @Parameter(name = "Force topic") @QueryParam("topic") Optional<String> topic,
                                          @Parameter(name = "Number of feedback documents", schema = @Schema(minimum = "1", maximum = "1000")) @QueryParam("fbDocs") @DefaultValue("50") Integer fbDocs,
                                          @Parameter(name = "Number of feedback terms", schema = @Schema(minimum = "1", maximum = "1000")) @QueryParam("fbTerms") @DefaultValue("20") Integer fbTerms,
                                          @Parameter(name = "Original query weight", schema = @Schema(minimum = "1", maximum = "1")) @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                          @Parameter(name = "Number of feedback collections") @QueryParam("fbCols") @DefaultValue("3") Integer fbCols,
                                          @Parameter(hidden = true) @QueryParam("fbMerge") @DefaultValue("false") Boolean fbMerge,
                                          @Parameter(name = "Use topics") @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                          @Parameter(name = "Use temporal reranking") @QueryParam("temporal") @DefaultValue("false") Boolean temporal,
                                          @Parameter(hidden = true) @QueryParam("rerank") @DefaultValue("true") Boolean rerank,
                                          @Parameter(hidden = true) @Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        try {
            long startTime = System.currentTimeMillis();
            String query = URLDecoder.decode(q, StandardCharsets.UTF_8);
            String filterQuery = URLDecoder.decode(fq.orElse(""), StandardCharsets.UTF_8);
            long[] epochs = Epochs.parseEpoch(epoch);

            SelectionTopDocuments shardResults = shardsManager.search(maxId, epochs, sRetweets, sFuture, limit, topics, query, filterQuery, null);
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
            if (temporal) {
                cascade.add(new RMTSReranker("mf.model", query, queryEpoch, (List<StatusDocument>) shardResults.scoreDocs, trecMicroblogAPIWrapper.getAnalyzer(), trecMicroblogAPIWrapper.getCollectionStats(), limit, limit, rerank));
            }
//            cascade.add(new MaxTFFilter(5));

            RerankerContext context = new RerankerContext(null, null, "MB000", query,
                    queryEpoch, Lists.newArrayList(), IndexStatuses.StatusField.TEXT.name, null);
            results.scoreDocs = cascade.run((List<StatusDocument>) results.scoreDocs, context);

            int totalFbDocs = shardResults.totalHits;
            int totalHits = results.totalHits;
            if (totalHits == 0) {
                throw new NotFoundException("No results found");
            }

            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

            ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
            SelectionFeedbackDocumentsResponse documentsResponse = new SelectionFeedbackDocumentsResponse(null, method, totalFbDocs, fbTerms, shardsFV.getMap().entrySet(), feedbackFV != null ? feedbackFV.getMap().entrySet() : null, fbVector.getMap().entrySet(), 0, null, shardResults, results);
            return new SelectionSearchResponse(responseHeader, documentsResponse);
        } catch (ParseException pe) {
            throw new BadRequestException(pe.getClass().getSimpleName());
        } catch (IOException | TException | ClassNotFoundException ioe) {
            throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
