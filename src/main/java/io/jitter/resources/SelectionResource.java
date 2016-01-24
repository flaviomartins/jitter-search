package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.Document;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/select")
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
    public SelectionResponse search(@QueryParam("q") Optional<String> q,
                                    @QueryParam("fq") Optional<String> fq,
                                    @QueryParam("limit") @DefaultValue("50") IntParam limit,
                                    @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                    @QueryParam("maxId") Optional<Long> maxId,
                                    @QueryParam("epoch") Optional<String> epoch,
                                    @QueryParam("method") @DefaultValue("crcsexp") String method,
                                    @QueryParam("topics") @DefaultValue("false") BooleanParam topics,
                                    @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                    @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                    @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        SelectionTopDocuments selectResults = null;

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                selectResults = selectionManager.search(query, limit.get(), !retweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                selectResults = selectionManager.search(query, limit.get(), !retweets.get(), epochs[0], epochs[1]);
            } else {
                selectResults = selectionManager.search(query, limit.get(), !retweets.get());
            }
        }

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        String methodName = selectionMethod.getClass().getSimpleName();

        List<Document> topSelDocs = selectResults.scoreDocs.subList(0, Math.min(limit.get(), selectResults.scoreDocs.size()));

        Map<String, Double> rankedCollections;
        if (!topics.get()) {
            rankedCollections = selectionManager.getRanked(selectionMethod, topSelDocs, normalize.get());
        } else {
            rankedCollections = selectionManager.getRankedTopics(selectionMethod, topSelDocs, normalize.get());
        }
        
        Map<String, Double> selectedCollections = selectionManager.limit(selectionMethod, rankedCollections, maxCol.get(), minRanks);

        long endTime = System.currentTimeMillis();

        int totalHits = selectResults.totalHits;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(selectedCollections, methodName, 0, selectResults);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
