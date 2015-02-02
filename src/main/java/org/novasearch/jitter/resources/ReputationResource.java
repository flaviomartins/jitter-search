package org.novasearch.jitter.resources;


import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.apache.log4j.Logger;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.reputation.InnerReputationResponse;
import org.novasearch.jitter.api.reputation.ReputationResponse;
import org.novasearch.jitter.core.ReputationReader;

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
import java.util.concurrent.atomic.AtomicLong;

@Path("/reputation")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class ReputationResource {
    private static final Logger logger = Logger.getLogger(ReputationResource.class);

    private final AtomicLong counter;
    private final ReputationReader reputationReader;

    public ReputationResource(ReputationReader reputationReader) throws IOException {
        counter = new AtomicLong();
        this.reputationReader = reputationReader;
    }

    @GET
    @Timed
    public ReputationResponse search(@QueryParam("q") Optional<String> query, @Context UriInfo uriInfo) throws IOException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String queryText = URLDecoder.decode(query.or(""), "UTF-8");

        long startTime = System.currentTimeMillis();

        double reputation = reputationReader.getReputation(queryText);

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), queryText));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        InnerReputationResponse innerReputationResponse = new InnerReputationResponse(reputation);
        return new ReputationResponse(responseHeader, innerReputationResponse);
    }

}
