package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.api.selection.SelectionDocumentsResponse;
import org.novasearch.jitter.api.selection.SelectionResponse;
import org.novasearch.jitter.core.selection.SelectionManager;
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;
import org.novasearch.jitter.core.selection.taily.TailyManager;

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
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;

@Path("/taily")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TailyResource {
    private static final Logger logger = Logger.getLogger(TailyResource.class);

    private final AtomicLong counter;
    private final TailyManager tailyManager;

    public TailyResource(TailyManager tailyManager) throws IOException {
        Preconditions.checkNotNull(tailyManager);

        counter = new AtomicLong();
        this.tailyManager = tailyManager;
    }

    @GET
    @Timed
    public SelectionResponse search(@QueryParam("q") Optional<String> q,
                                    @QueryParam("method") Optional<String> method,
                                    @QueryParam("limit") Optional<Integer> limit,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        int n = limit.or(50);

        long startTime = System.currentTimeMillis();

        Map<String, Double> ranked = tailyManager.getRanked(query);

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(ranked, "Taily", 0, 0, null);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
