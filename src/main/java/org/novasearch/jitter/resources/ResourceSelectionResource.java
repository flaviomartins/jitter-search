package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.rs.ResourceSelectionDocumentsResponse;
import org.novasearch.jitter.api.rs.ResourceSelectionResponse;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.rs.ResourceSelection;
import org.novasearch.jitter.rs.ResourceSelectionMethod;
import org.novasearch.jitter.rs.ResourceSelectionMethodFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;

@Path("/rs")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class ResourceSelectionResource {
    private static final Logger logger = Logger.getLogger(SearchResource.class);

    private final AtomicLong counter;
    private final ResourceSelection resourceSelection;

    public ResourceSelectionResource(ResourceSelection resourceSelection) throws IOException {
        Preconditions.checkNotNull(resourceSelection);

        counter = new AtomicLong();
        this.resourceSelection = resourceSelection;
    }

    @GET
    @Timed
    public ResourceSelectionResponse search(@QueryParam("q") Optional<String> query,
                                            @QueryParam("method") Optional<String> method,
                                            @QueryParam("limit") Optional<Integer> limit,
                                            @Context UriInfo uriInfo)
            throws IOException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String queryText = URLDecoder.decode(query.or(""), "UTF-8");
        String methodText = method.or(resourceSelection.getMethod());
        int queryLimit = limit.or(50);

        long startTime = System.currentTimeMillis();

        List<Document> results = resourceSelection.search(queryText, queryLimit);
        int totalHits = results != null ? results.size() : 0;

        ResourceSelectionMethod resourceSelectionMethod = ResourceSelectionMethodFactory.getMethod(methodText);
        String methodName = resourceSelectionMethod.getClass().getSimpleName();

        SortedMap<String, Float> ranked = resourceSelection.getRanked(resourceSelectionMethod, results);

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), queryText));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        ResourceSelectionDocumentsResponse documentsResponse = new ResourceSelectionDocumentsResponse(ranked, methodName, totalHits, 0, results); // docs
        return new ResourceSelectionResponse(responseHeader, documentsResponse);
    }
}
