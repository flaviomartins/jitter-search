package org.novasearch.jitter.resources;

import cc.twittertools.index.IndexStatuses;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.api.search.DocumentsResponse;
import org.novasearch.jitter.api.search.SearchResponse;
import org.novasearch.jitter.core.DocumentComparable;

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
import java.net.URLDecoder;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

@Path("/search")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SearchResource {
    private static final Logger logger = Logger.getLogger(SearchResource.class);

    private static QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);

    private final AtomicLong counter;
    private final IndexReader reader;
    private final IndexSearcher searcher;

    public SearchResource(File indexPath) throws IOException {
        Preconditions.checkNotNull(indexPath);
        Preconditions.checkArgument(indexPath.exists());

        counter = new AtomicLong();
        reader = DirectoryReader.open(FSDirectory.open(indexPath));
        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
    }

    @GET
    @Timed
    public SearchResponse search(@QueryParam("q") Optional<String> query,
                                 @QueryParam("fq") Optional<String> filterQuery,
                                 @QueryParam("limit") Optional<Integer> limit,
                                 @QueryParam("max_id") Optional<Long> max_id,
                                 @QueryParam("epoch") Optional<String> epoch_range,
                                 @QueryParam("filter_rt") Optional<Boolean> filter_rt,
                                 @Context UriInfo uriInfo)
            throws IOException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        String queryText = URLDecoder.decode(query.or(""), "UTF-8");
        int queryLimit = limit.or(1000);
        boolean filterRT = filter_rt.or(false);

        int totalHits;
        int numResults;
        List<Document> results = Lists.newArrayList();
        long startTime = System.currentTimeMillis();

        try {
            Query q = QUERY_PARSER.parse(queryText);
            numResults = queryLimit > 10000 ? 10000 : queryLimit;


            TopDocs rs = null;
            if (max_id.isPresent()) {
                Filter filter =
                        NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, max_id.get(), true, true);
                rs = searcher.search(q, filter, numResults);
            } else if (epoch_range.isPresent()) {
                long first_epoch = 0L;
                long last_epoch = Long.MAX_VALUE;
                String[] epochs = epoch_range.get().split("[: ]");
                try {
                    first_epoch = Long.parseLong(epochs[0]);
                    last_epoch = Long.parseLong(epochs[1]);
                } catch (Exception e) {
                    // pass
                }
                Filter filter =
                        NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, first_epoch, last_epoch, true, true);
                rs = searcher.search(q, filter, numResults);
            } else {
                rs = searcher.search(q, numResults);
            }
            totalHits = rs.totalHits;
            for (ScoreDoc scoreDoc : rs.scoreDocs) {
                org.apache.lucene.document.Document hit = searcher.doc(scoreDoc.doc);

                Document p = new Document();
                p.id = (Long) hit.getField(IndexStatuses.StatusField.ID.name).numericValue();
                p.screen_name = hit.get(IndexStatuses.StatusField.SCREEN_NAME.name);
                p.epoch = (Long) hit.getField(IndexStatuses.StatusField.EPOCH.name).numericValue();
                p.text = hit.get(IndexStatuses.StatusField.TEXT.name);
                p.rsv = scoreDoc.score;

                p.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
                p.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();

                if (hit.get(IndexStatuses.StatusField.LANG.name) != null) {
                    p.lang = hit.get(IndexStatuses.StatusField.LANG.name);
                }

                if (hit.get(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name) != null) {
                    p.in_reply_to_status_id = (Long) hit.getField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name).numericValue();
                }

                if (hit.get(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name) != null) {
                    p.in_reply_to_user_id = (Long) hit.getField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name).numericValue();
                }

                if (hit.get(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name) != null) {
                    p.retweeted_status_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name).numericValue();
                }

                if (hit.get(IndexStatuses.StatusField.RETWEETED_USER_ID.name) != null) {
                    p.retweeted_user_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_USER_ID.name).numericValue();
                }

                if (hit.get(IndexStatuses.StatusField.RETWEET_COUNT.name) != null) {
                    p.retweeted_count = (Integer) hit.getField(IndexStatuses.StatusField.RETWEET_COUNT.name).numericValue();
                }

                results.add(p);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage()); // replace by specific exception
        }

        int retweetCount = 0;
        SortedSet<DocumentComparable> sortedResults = new TreeSet<DocumentComparable>();
        for (Document p : results) {
            // Throw away retweets.
            if (filterRT && p.getRetweeted_status_id() != 0) {
                retweetCount++;
                continue;
            }

            sortedResults.add(new DocumentComparable(p));
        }
        if (filterRT) {
            logger.info("filter_rt count: " + retweetCount);
            totalHits -= retweetCount;
        }

        List<Document> docs = Lists.newArrayList();

        int i = 1;
        int dupliCount = 0;
        double rsvPrev = 0;
        for (DocumentComparable sortedResult : sortedResults) {
            Document result = sortedResult.getDocument();
            double rsvCurr = result.rsv;
            if (Math.abs(rsvCurr - rsvPrev) > 0.0000001) {
                dupliCount = 0;
            } else {
                dupliCount++;
                rsvCurr = rsvCurr - 0.000001 / numResults * dupliCount;
            }

            docs.add(new Document(result));
            i++;
            rsvPrev = result.rsv;
        }

        long endTime = System.currentTimeMillis();
        logger.info(String.format("%4dms %s", (endTime - startTime), queryText));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        DocumentsResponse documentsResponse = new DocumentsResponse(totalHits, 0, docs);
        return new SearchResponse(responseHeader, documentsResponse);
    }
}
