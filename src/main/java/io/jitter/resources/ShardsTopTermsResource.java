package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.collectionstatistics.TermsResponse;
import io.jitter.api.collectionstatistics.TopTermsResponse;
import io.jitter.core.shards.ShardsManager;
import org.apache.lucene.misc.TermStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

@Path("/shards/top/terms")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class ShardsTopTermsResource {
    private static final Logger logger = LoggerFactory.getLogger(ShardsTopTermsResource.class);

    private final AtomicLong counter;
    private final ShardsManager shardsManager;

    public ShardsTopTermsResource(ShardsManager shardsManager) throws IOException {
        Preconditions.checkNotNull(shardsManager);

        counter = new AtomicLong();
        this.shardsManager = shardsManager;
    }

    @GET
    @Timed
    public TopTermsResponse top(@QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                @Context UriInfo uriInfo) throws Exception {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        long startTime = System.currentTimeMillis();

        TermStats[] terms = shardsManager.getHighFreqTerms(limit.get());

        long endTime = System.currentTimeMillis();

        int totalHits = terms != null ? terms.length : 0;

        logger.info(String.format(Locale.ENGLISH, "%4dms %d high freq terms", (endTime - startTime), limit.get()));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        TermsResponse termsResponse = new TermsResponse(totalHits, 0, terms);
        return new TopTermsResponse(responseHeader, termsResponse);
    }
}
