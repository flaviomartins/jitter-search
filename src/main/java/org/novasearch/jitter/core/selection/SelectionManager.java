package org.novasearch.jitter.core.selection;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.core.search.DocumentComparable;
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;
import org.novasearch.jitter.core.twitter.manager.TwitterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SelectionManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManager.class);

    public static final int MAX_RESULTS = 10000;

    private static final QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);

    private DirectoryReader reader;
    private IndexSearcher searcher;

    private final String indexPath;
    private final String method;
    private final boolean removeDuplicates;
    private Map<String, ImmutableSortedSet<String>> topics;
    private TwitterManager twitterManager;

    public SelectionManager(String indexPath, String method, boolean removeDuplicates, Map<String, Set<String>> topics) {
        this.indexPath = indexPath;
        this.method = method;
        this.removeDuplicates = removeDuplicates;
        TreeMap<String, ImmutableSortedSet<String>> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : topics.entrySet()) {
            treeMap.put(entry.getKey(), new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER).addAll(entry.getValue()).build());
        }
        this.topics = treeMap;
    }

    @Override
    public void start() throws Exception {
        try {
            reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
            searcher = new IndexSearcher(reader);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    public String getMethod() {
        return method;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public TwitterManager getTwitterManager() {
        return twitterManager;
    }

    public Map<String, ImmutableSortedSet<String>> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, ImmutableSortedSet<String>> topics) {
        this.topics = topics;
    }

    public boolean isRemoveDuplicates() {
        return removeDuplicates;
    }

    public void setTwitterManager(TwitterManager twitterManager) {
        this.twitterManager = twitterManager;
    }

    public SortedMap<String, Double> getRanked(List<Document> results) {
        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        return getSortedMap(selectionMethod.getRanked(results));
    }

    public SortedMap<String, Double> getRanked(SelectionMethod selectionMethod, List<Document> results) {
        return getSortedMap(selectionMethod.getRanked(results));
    }

    public SortedMap<String, Double> getRankedTopics(SelectionMethod selectionMethod, List<Document> results) {
        Map<String, Double> ranked = selectionMethod.getRanked(results);
        Map<String, Double> map = new HashMap<>();
        for (String topic : topics.keySet()) {
            double sum = 0;
            for (String collection : topics.get(topic)) {
                for (String col : ranked.keySet()) {
                    if (col.equalsIgnoreCase(collection))
                        sum += ranked.get(col);
                }
            }
            if (sum != 0)
                map.put(topic, sum);
        }
        return getSortedMap(map);
    }

    private SortedMap<String, Double> getSortedMap(Map<String, Double> map) {
        SelectionComparator comparator = new SelectionComparator(map);
        TreeMap<String, Double> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(map);
        return sortedMap;
    }

    public List<Document> filterTopic(String topicName, List<Document> selectResults) {
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults) {
            if (topics.get(topicName) != null && topics.get(topicName).contains(doc.getScreen_name())) {
                results.add(doc);
            }
        }
        return results;
    }

    public List<Document> searchTopic(String topicName, String query, int n, boolean filterRT) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        TopDocs rs = getSearcher().search(q, numResults);

        List<Document> sorted = getSorted(rs, filterRT);

        return filterTopic(topicName, sorted);
    }

    public List<Document> search(String query, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        TopDocs rs = getSearcher().search(q, filter, numResults);

        return getSorted(rs, filterRT);
    }

    public List<Document> search(String query, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        TopDocs rs = getSearcher().search(q, filter, numResults);

        return getSorted(rs, filterRT);
    }

    public List<Document> search(String query, int n, boolean filterRT) throws IOException, ParseException {
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
        Query q = QUERY_PARSER.parse(query);

        TopDocs rs = getSearcher().search(q, numResults);

        return getSorted(rs, filterRT);
    }

    public List<Document> search(String query, int n) throws IOException, ParseException {
        int numResults = n > 10000 ? 10000 : n;
        Query q = QUERY_PARSER.parse(query);

        TopDocs rs = getSearcher().search(q, numResults);

        return getSorted(rs, false);
    }

    private List<Document> getResults(TopDocs rs) throws IOException {
        List<Document> results = Lists.newArrayList();
        for (ScoreDoc scoreDoc : rs.scoreDocs) {
            org.apache.lucene.document.Document hit = getSearcher().doc(scoreDoc.doc);

            Document p = new Document();
            p.id = (Long) hit.getField(IndexStatuses.StatusField.ID.name).numericValue();
            p.screen_name = hit.get(IndexStatuses.StatusField.SCREEN_NAME.name);
            p.epoch = (Long) hit.getField(IndexStatuses.StatusField.EPOCH.name).numericValue();
            p.text = hit.get(IndexStatuses.StatusField.TEXT.name);
            p.rsv = scoreDoc.score;

            if (hit.get(IndexStatuses.StatusField.FOLLOWERS_COUNT.name) != null) {
                p.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.STATUSES_COUNT.name) != null) {
                p.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();
            }

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
            logger.info("filter_rt count: {}", retweetCount);
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
            // FIXME: what is this?
            result.rsv = rsvCurr;

            docs.add(new Document(result));
            i++;
            rsvPrev = result.rsv;
        }

        return docs;
    }

    public void index() throws IOException {
        logger.info("standard index");
        twitterManager.index(indexPath, removeDuplicates);
    }

    public IndexSearcher getSearcher() throws IOException {
        try {
            if (reader == null) {
                reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
                searcher = new IndexSearcher(reader);
                searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
            } else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                    searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
                }
            }
        } catch (IndexNotFoundException e) {
            logger.error(e.getMessage());
        }
        return searcher;
    }
}
