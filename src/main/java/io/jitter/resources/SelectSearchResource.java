package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/ss")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectSearchResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(SelectSearchResource.class);

    private final AtomicLong counter;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;
    private final TailyManager tailyManager;

    public SelectSearchResource(SelectionManager selectionManager, ShardsManager shardsManager, TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
        this.tailyManager = tailyManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    public SelectionSearchResponse search(@QueryParam("q") @NotEmpty String q,
                                          @QueryParam("fq") Optional<String> fq,
                                          @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                          @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                          @QueryParam("maxId") Optional<Long> maxId,
                                          @QueryParam("epoch") Optional<String> epoch,
                                          @QueryParam("sLimit") @DefaultValue("50") Integer sLimit,
                                          @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                          @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                          @QueryParam("method") @DefaultValue("ranks") String method,
                                          @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                          @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                          @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                          @QueryParam("v") @DefaultValue("10") Integer v,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q, "UTF-8");
        String filterQuery = URLDecoder.decode(fq.orElse(""), "UTF-8");
        long[] epochs = Epochs.parseEpoch(epoch);

        long startTime = System.currentTimeMillis();

        Selection selection;
        if ("taily".equalsIgnoreCase(method)) {
            selection = tailyManager.selection(query, v, topics);
        } else {
            selection = selectionManager.selection(query, filterQuery, maxId, epochs, sLimit, sRetweets, sFuture,
                    method, maxCol, minRanks, normalize, topics);
        }

        Set<String> selected = selection.getCollections().keySet();

        SelectionTopDocuments shardResults = shardsManager.search(maxId, epochs, retweets, sFuture, limit, topics, query, filterQuery, selected);

        long endTime = System.currentTimeMillis();

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), shardResults.totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionSearchDocumentsResponse documentsResponse = new SelectionSearchDocumentsResponse(selection.getCollections().entrySet(), method, 0, selection.getResults(), shardResults);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
