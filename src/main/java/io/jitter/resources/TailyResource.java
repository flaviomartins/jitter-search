package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.selection.SelectionDocumentsResponse;
import io.jitter.api.selection.SelectionResponse;
import io.jitter.core.selection.taily.TailyManager;
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

@Path("/taily")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TailyResource {
    private static final Logger logger = LoggerFactory.getLogger(TailyResource.class);

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
                                    @QueryParam("v") @DefaultValue("50") IntParam v,
                                    @QueryParam("topics") @DefaultValue("false") BooleanParam topics,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        Map<String, Double> map = new LinkedHashMap<>();

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            Map<String, Double> ranking;
            if (topics.get()) {
                ranking = tailyManager.getRankedTopics(query);
            } else {
                ranking = tailyManager.getRanked(query);
            }

            for (Map.Entry<String, Double> entry : ranking.entrySet()) {
                if (entry.getValue() >= v.get()) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(map, "Taily", 0, 0, null);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
