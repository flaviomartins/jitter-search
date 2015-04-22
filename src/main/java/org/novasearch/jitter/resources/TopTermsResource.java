package org.novasearch.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.lucene.misc.TermStats;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.collectionstatistics.TermsResponse;
import org.novasearch.jitter.api.collectionstatistics.TopTermsResponse;
import org.novasearch.jitter.core.search.SearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@Path("/top/terms")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TopTermsResource {
    private static final Logger logger = LoggerFactory.getLogger(TopTermsResource.class);

    private final AtomicLong counter;
    private final SearchManager searchManager;

    public TopTermsResource(SearchManager searchManager) throws IOException {
        Preconditions.checkNotNull(searchManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
    }

    @GET
    @Timed
    public TopTermsResponse top(@QueryParam("limit") Optional<Integer> limit, @Context UriInfo uriInfo) throws Exception {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        int n = limit.or(1000);

        long startTime = System.currentTimeMillis();

        TermStats[] terms = searchManager.getHighFreqTerms(n);

        int totalHits = terms != null ? terms.length : 0;

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %d high freq terms", (endTime - startTime), n));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        TermsResponse termsResponse = new TermsResponse(totalHits, 0, terms);
        return new TopTermsResponse(responseHeader, termsResponse);
    }
}
