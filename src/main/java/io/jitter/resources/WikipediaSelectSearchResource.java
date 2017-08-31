package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.jersey.caching.CacheControl;
import io.jitter.api.ResponseHeader;
import io.jitter.api.search.SelectionSearchDocumentsResponse;
import io.jitter.api.search.SelectionSearchResponse;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.wikipedia.WikipediaSelectionManager;
import io.jitter.core.wikipedia.WikipediaShardsManager;
import org.apache.lucene.queryparser.classic.ParseException;
import org.hibernate.validator.constraints.NotEmpty;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/wikipedia/ss")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class WikipediaSelectSearchResource extends AbstractFeedbackResource {
    private static final Logger logger = LoggerFactory.getLogger(WikipediaSelectSearchResource.class);

    private final AtomicLong counter;
    private final WikipediaSelectionManager wikipediaSelectionManager;
    private final WikipediaShardsManager wikipediaShardsManager;

    public WikipediaSelectSearchResource(WikipediaSelectionManager wikipediaSelectionManager, WikipediaShardsManager wikipediaShardsManager) throws IOException {
        Preconditions.checkNotNull(wikipediaSelectionManager);
        Preconditions.checkNotNull(wikipediaShardsManager);

        counter = new AtomicLong();
        this.wikipediaSelectionManager = wikipediaSelectionManager;
        this.wikipediaShardsManager = wikipediaShardsManager;
    }

    @GET
    @Timed
    @CacheControl(maxAge = 1, maxAgeUnit = TimeUnit.HOURS)
    public SelectionSearchResponse search(@QueryParam("q") @NotEmpty String q,
                                          @QueryParam("fq") Optional<String> fq,
                                          @QueryParam("limit") @DefaultValue("1000") Integer limit,
                                          @QueryParam("retweets") @DefaultValue("false") Boolean retweets,
                                          @QueryParam("maxId") Optional<Long> maxId,
                                          @QueryParam("epoch") Optional<String> epoch,
                                          @QueryParam("sLimit") @DefaultValue("50") Integer sLimit,
                                          @QueryParam("sRetweets") @DefaultValue("true") Boolean sRetweets,
                                          @QueryParam("sFuture") @DefaultValue("false") Boolean sFuture,
                                          @QueryParam("method") @DefaultValue("ranks") String method,
                                          @QueryParam("topics") @DefaultValue("true") Boolean topics,
                                          @QueryParam("maxCol") @DefaultValue("3") Integer maxCol,
                                          @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                          @QueryParam("normalize") @DefaultValue("true") Boolean normalize,
                                          @QueryParam("v") @DefaultValue("10") Integer v,
                                          @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String query = URLDecoder.decode(q, "UTF-8");

        long startTime = System.currentTimeMillis();

        Selection selection = wikipediaSelectionManager.selection(sLimit, method, maxCol, minRanks, normalize, query, false, topics);

        Set<String> selected = selection.getCollections().keySet();

        SelectionTopDocuments shardResults = wikipediaShardsManager.search(topics, selected, query, limit, false);

        long endTime = System.currentTimeMillis();

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), shardResults.totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectionSearchDocumentsResponse documentsResponse = new SelectionSearchDocumentsResponse(selection.getCollections().entrySet(), method, 0, selection.getResults(), shardResults);
        return new SelectionSearchResponse(responseHeader, documentsResponse);
    }
}
