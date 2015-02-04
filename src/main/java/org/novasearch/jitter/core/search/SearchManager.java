package org.novasearch.jitter.core.search;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.core.DocumentComparable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class SearchManager implements Managed {
    private static final Logger logger = Logger.getLogger(SearchManager.class);

    private static final QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);
    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private final String index;
    private final String database;

    private IndexReader reader;
    private IndexSearcher searcher;

    public SearchManager(String index, String database) throws IOException {
        this.index = index;
        this.database = database;
        reader = DirectoryReader.open(FSDirectory.open(new File(index)));
        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        reader.close();
    }

    public List<Document> search(String query, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        TopDocs rs = searcher.search(q, filter, numResults);

        return getSorted(rs, filterRT);
    }

    public List<Document> search(String query, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        TopDocs rs = searcher.search(q, filter, numResults);

        return getSorted(rs, filterRT);
    }

    public List<Document> search(String query, int n, boolean filterRT) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        TopDocs rs = searcher.search(q, numResults);

        return getSorted(rs, filterRT);
    }

    private List<Document> getResults(TopDocs rs) throws IOException {
        List<Document> results = Lists.newArrayList();
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

        return results;
    }

    public List<Document> getSorted(TopDocs rs, boolean filterRT) throws IOException {
        List<Document> results = getResults(rs);
        return sortResults(results, filterRT);
    }

    private List<Document> sortResults(List<Document> results, boolean filterRT) {
        int retweetCount = 0;
        SortedSet<DocumentComparable> sortedResults = new TreeSet<>();
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
                rsvCurr = rsvCurr - 0.000001 / results.size() * dupliCount;
            }

            docs.add(new Document(result));
            i++;
            rsvPrev = result.rsv;
        }

        return docs;
    }

    public void index() {

    }


    public String getIndex() {
        return index;
    }

    public String getDatabase() {
        return database;
    }

    public TermStats[] getHighFreqTerms(int n) throws Exception {
        int numResults = n > MAX_TERMS_RESULTS ? MAX_TERMS_RESULTS : n;
        return HighFreqTerms.getHighFreqTerms(reader, numResults, IndexStatuses.StatusField.TEXT.name);
    }
}
