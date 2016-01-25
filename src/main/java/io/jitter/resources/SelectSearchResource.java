package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.utils.Epochs;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Path("/ss")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectSearchResource {
    private static final Logger logger = LoggerFactory.getLogger(SelectSearchResource.class);

    private final AtomicLong counter;
    private final SelectionManager selectionManager;
    private final ShardsManager shardsManager;

    public SelectSearchResource(SelectionManager selectionManager, ShardsManager shardsManager) throws IOException {
        Preconditions.checkNotNull(selectionManager);
        Preconditions.checkNotNull(shardsManager);

        counter = new AtomicLong();
        this.selectionManager = selectionManager;
        this.shardsManager = shardsManager;
    }

    @GET
    @Timed
    public SelectionSearchResponse search(@QueryParam("q") Optional<String> q,
                                          @QueryParam("fq") Optional<String> fq,
                                          @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                          @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                          @QueryParam("maxId") Optional<Long> maxId,
                                          @QueryParam("epoch") Optional<String> epoch,
                                          @QueryParam("sLimit") @DefaultValue("50") IntParam sLimit,
                                          @QueryParam("sRetweets") @DefaultValue("true") BooleanParam sRetweets,
                                          @QueryParam("method") @DefaultValue("crcsexp") String method,
                                          @QueryParam("topics") @DefaultValue("true") BooleanParam topics,
                                          @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                          @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");

        long startTime = System.currentTimeMillis();

        SelectionTopDocuments selectResults = null;
        if (q.isPresent()) {
            if (maxId.isPresent()) {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), epochs[0], epochs[1]);
            } else {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get());
            }
        }

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        String methodName = selectionMethod.getClass().getSimpleName();
        Map<String, Double> selectedSources = selectionManager.select(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
        Map<String, Double> selectedTopics = selectionManager.selectTopics(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());

        Set<String> selected = topics.get() ? selectedTopics.keySet() : selectedSources.keySet();

        SelectionTopDocuments shardResults = null;
        if (q.isPresent()) {
            if (maxId.isPresent()) {
                shardResults = shardsManager.search(topics.get(), selected, query, limit.get(), !retweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                shardResults = shardsManager.search(topics.get(), selected, query, limit.get(), !retweets.get(), epochs[0], epochs[1]);
            } else {
                shardResults = shardsManager.search(topics.get(), selected, query, limit.get(), !retweets.get());
            }
        }

        long endTime = System.currentTimeMillis();

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), shardResults.totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionSearchDocumentsResponse documentsResponse = new SelectionSearchDocumentsResponse(selectedSources, selectedTopics, methodName, 0, selectResults, shardResults);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
