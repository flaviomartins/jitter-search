package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
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
                                    @QueryParam("sLimit") @DefaultValue("50") IntParam sLimit,
                                    @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                    @QueryParam("maxId") Optional<Long> maxId,
                                    @QueryParam("epoch") Optional<String> epoch,
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
                selectResults = selectionManager.search(query, sLimit.get(), !retweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                long[] epochs = Epochs.parseEpochRange(epoch.get());
                selectResults = selectionManager.search(query, sLimit.get(), !retweets.get(), epochs[0], epochs[1]);
            } else {
                selectResults = selectionManager.search(query, sLimit.get(), !retweets.get());
            }
        }

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        String methodName = selectionMethod.getClass().getSimpleName();
        
        Map<String, Double> selected;
        if (!topics.get()) {
            selected = selectionManager.select(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
        } else {
            selected = selectionManager.selectTopics(selectResults, sLimit.get(), selectionMethod, maxCol.get(), minRanks, normalize.get());
        }
        
        long endTime = System.currentTimeMillis();

        int totalHits = selectResults.totalHits;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(selected, methodName, 0, selectResults);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
