package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.selection.SelectionDocumentsResponse;
import org.novasearch.jitter.api.selection.SelectionResponse;
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
import java.util.*;
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
                                    @QueryParam("v") Optional<Integer> v,
                                    @QueryParam("topics") Optional<Boolean> topics,
                                    @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.or(""), "UTF-8");
        int taily_v = v.or(50);
        boolean retTopics = topics.or(false);

        long startTime = System.currentTimeMillis();

        Map<String, Double> ranking;
        if (retTopics) {
            ranking = tailyManager.getRankedTopics(query);
        } else {
            ranking = tailyManager.getRanked(query);
        }


        Map<String, Double> map = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : ranking.entrySet()) {
            if (entry.getValue() >= taily_v) {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionDocumentsResponse documentsResponse = new SelectionDocumentsResponse(map, "Taily", 0, 0, null);
        return new SelectionResponse(responseHeader, documentsResponse);
    }
}
