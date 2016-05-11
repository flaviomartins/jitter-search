package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.DocumentsResponse;
import io.jitter.api.search.SearchResponse;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Path("/trecsearch")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TrecSearchResource {
    private static final Logger logger = LoggerFactory.getLogger(TrecSearchResource.class);

    private final AtomicLong counter;
    private final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper;

    public TrecSearchResource(TrecMicroblogAPIWrapper trecMicroblogAPIWrapper) throws IOException {
        Preconditions.checkNotNull(trecMicroblogAPIWrapper);

        counter = new AtomicLong();
        this.trecMicroblogAPIWrapper = trecMicroblogAPIWrapper;
    }

    @GET
    @Timed
    public SearchResponse search(@QueryParam("q") Optional<String> q,
                                 @QueryParam("fq") Optional<String> fq,
                                 @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                 @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                 @QueryParam("maxId") Optional<Long> maxId,
                                 @QueryParam("epoch") Optional<String> epoch,
                                 @Context UriInfo uriInfo)
            throws IOException, ParseException, TException, ClassNotFoundException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q.orElse(""), "UTF-8");

        long startTime = System.currentTimeMillis();

        TopDocuments results = trecMicroblogAPIWrapper.search(limit, retweets, maxId, query);

        long endTime = System.currentTimeMillis();

        int totalHits = results != null ? results.totalHits : 0;
        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        DocumentsResponse documentsResponse = new DocumentsResponse(totalHits, 0, results);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
