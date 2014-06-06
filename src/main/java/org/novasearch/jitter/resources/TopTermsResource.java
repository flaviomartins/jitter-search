package org.novasearch.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.store.FSDirectory;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.collectionstatistics.TermsResponse;
import org.novasearch.jitter.api.collectionstatistics.TopTermsResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@Path("/top/terms")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TopTermsResource {
    private static final Logger LOG = Logger.getLogger(TopTermsResource.class);

    private final AtomicLong counter;
    private final IndexReader reader;

    public TopTermsResource(File indexPath) throws IOException {
        Preconditions.checkNotNull(indexPath);
        Preconditions.checkArgument(indexPath.exists());

        counter = new AtomicLong();
        reader = DirectoryReader.open(FSDirectory.open(indexPath));
    }

    @GET
    @Timed
    public TopTermsResponse top(@QueryParam("limit") Optional<Integer> limit, @Context UriInfo uriInfo) throws Exception {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        int termsLimit = limit.or(1000);
        int termsLimitCapped = termsLimit > 10000 ? 10000 : termsLimit;

        long startTime = System.currentTimeMillis();
        TermStats[] terms = HighFreqTerms.getHighFreqTerms(reader, termsLimitCapped, IndexStatuses.StatusField.TEXT.name);
        long endTime = System.currentTimeMillis();

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        TermsResponse termsResponse = new TermsResponse(terms.length, 0, terms);
        return new TopTermsResponse(responseHeader, termsResponse);
    }
}
