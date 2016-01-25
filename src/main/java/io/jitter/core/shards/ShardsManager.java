package io.jitter.core.shards;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.search.Document;
import io.jitter.core.search.DocumentComparable;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.similarities.IDFSimilarity;
import io.jitter.core.twitter.manager.TwitterManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ShardsManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(ShardsManager.class);
    
    public static final int MAX_RESULTS = 10000;

    private static final Analyzer analyzer = IndexStatuses.ANALYZER;
    private static final QueryParser QUERY_PARSER =
            new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer);
    public static final IDFSimilarity SIMILARITY = new IDFSimilarity();

    private DirectoryReader reader;
    private IndexSearcher searcher;

    private final String indexPath;
    private final String method;
    private final boolean removeDuplicates;
    private final boolean live;

    private Map<String, ImmutableSortedSet<String>> topics;

    private ShardStatsBuilder shardStatsBuilder;
    private Map<String, String> reverseTopicMap;
    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;

    private TwitterManager twitterManager;
    private TailyManager tailyManager;

    public ShardsManager(String indexPath, String method, boolean removeDuplicates, boolean live, Map<String, Set<String>> topics) {
        this.indexPath = indexPath;
        this.method = method;
        this.removeDuplicates = removeDuplicates;
        this.live = live;

        TreeMap<String, ImmutableSortedSet<String>> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : topics.entrySet()) {
            treeMap.put(entry.getKey(), new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER).addAll(entry.getValue()).build());
        }
        this.topics = treeMap;
    }

    @Override
    public void start() throws Exception {
        try {
            searcher = getSearcher();
            shardStatsBuilder = new ShardStatsBuilder(reader, topics);
            reverseTopicMap = shardStatsBuilder.getReverseTopicMap();
            collectStats();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void collectStats() throws IOException {
        shardStatsBuilder.collectStats();
        collectionsShardStats = shardStatsBuilder.getCollectionsShardStats();
        topicsShardStats = shardStatsBuilder.getTopicsShardStats();
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

    public Map<String, ImmutableSortedSet<String>> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, ImmutableSortedSet<String>> topics) {
        this.topics = topics;
    }

    public boolean isRemoveDuplicates() {
        return removeDuplicates;
    }

    public ShardStats getCollectionsShardStats() {
        return collectionsShardStats;
    }

    public ShardStats getTopicsShardStats() {
        return topicsShardStats;
    }

    public TwitterManager getTwitterManager() {
        return twitterManager;
    }

    public TailyManager getTailyManager() {
        return tailyManager;
    }

    public void setCollectionsShardStats(ShardStats collectionsShardStats) {
        this.collectionsShardStats = collectionsShardStats;
    }

    public void setTopicsShardStats(ShardStats topicsShardStats) {
        this.topicsShardStats = topicsShardStats;
    }

    public void setTwitterManager(TwitterManager twitterManager) {
        this.twitterManager = twitterManager;
    }

    public void setTailyManager(TailyManager tailyManager) {
        this.tailyManager = tailyManager;
    }

    private SelectionTopDocuments filter(Query query, Set<String> selectedSources, SelectionTopDocuments selectResults) {
        HashSet<String> collections = Sets.newHashSet(selectedSources);
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults.scoreDocs) {
            if (collections.contains(doc.getScreen_name())) {
                results.add(doc);
            }
        }

        int totalDF = 0;
        Set<Term> queryTerms = new TreeSet<>();
        query.extractTerms(queryTerms);
        for (Term term : queryTerms) {
            String text = term.text();
            if (text.isEmpty())
                continue;
            for (String selectedSource : selectedSources) {
                totalDF += tailyManager.getDF(selectedSource, text);
            }
        }

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(results.size(), results);
        selectionTopDocuments.setC_r(totalDF);
        return selectionTopDocuments;
    }

    private SelectionTopDocuments filterTopics(Query query, Set<String> selectedTopics, SelectionTopDocuments selectResults) {
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults.scoreDocs) {
            for (String selectedTopic: selectedTopics) {
                if (topics.get(selectedTopic) != null && topics.get(selectedTopic).contains(doc.getScreen_name())) {
                    results.add(doc);
                }
            }
        }

        int totalDF = 0;
        Set<Term> queryTerms = new TreeSet<>();
        query.extractTerms(queryTerms);
        for (Term term : queryTerms) {
            String text = term.text();
            if (text.isEmpty())
                continue;
            for (String selectedTopic : selectedTopics) {
                totalDF += tailyManager.getTopicsDF(selectedTopic, text);
            }
        }

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(results.size(), results);
        selectionTopDocuments.setC_r(totalDF);
        return selectionTopDocuments;
    }

    public SelectionTopDocuments limit(SelectionTopDocuments selectionTopDocuments, int limit) {
        selectionTopDocuments.scoreDocs = selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        return selectionTopDocuments;
    }

    public SelectionTopDocuments reScoreSelected(Iterable<Map.Entry<String, Double>> selectedTopics, List<Document> selectResults) {
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults) {
            Document updatedDocument = new Document(doc);
            for (Map.Entry<String, Double> selectedTopic : selectedTopics) {
                if (topics.get(selectedTopic.getKey()) != null && topics.get(selectedTopic.getKey()).contains(doc.getScreen_name())) {
                    double newRsv = selectedTopic.getValue() * doc.getRsv();
                    updatedDocument.setRsv(newRsv);
                    results.add(updatedDocument);
                }
            }
        }
        List<Document> documents = sortResults(results, results.size(), false);
        return new SelectionTopDocuments(documents.size(), documents);
    }

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, Filter filter, int n, boolean filterRT) throws IOException, ParseException {
        Query q = QUERY_PARSER.parse(query.replaceAll(",", ""));
        
        TopDocs rs;
        if (filter != null )
            rs = getSearcher().search(q, filter, reader.numDocs());
        else
            rs = getSearcher().search(q, reader.numDocs());

        List<Document> sorted = getSorted(rs, n, filterRT);

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(rs.totalHits, sorted);
        
        if (!topics) {
            return limit(filter(q, collections, selectionTopDocuments), n);
        } else {
            return limit(filterTopics(q, collections, selectionTopDocuments), n);
        }
    }

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(topics, collections, query, null, n, filterRT);
    }

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, int n) throws IOException, ParseException {
        return isearch(topics, collections, query, null, n, false);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        return isearch(topics, collections, query, filter, n, filterRT);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        return isearch(topics, collections, query, filter, n, filterRT);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(topics, collections, query, n, filterRT);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, int n) throws IOException, ParseException {
        return isearch(topics, collections, query, n, false);
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

    private List<Document> getSorted(TopDocs rs, int n, boolean filterRT) throws IOException {
        List<Document> results = getResults(rs);
        return sortResults(results, n, filterRT);
    }

    private List<Document> sortResults(List<Document> results, int n, boolean filterRT) {
        int count = 0;
        int retweetCount = 0;
        SortedSet<DocumentComparable> sortedResults = new TreeSet<>();
        for (Document p : results) {
            if (count >= n)
                break;

            // Throw away retweets.
            if (filterRT && p.getRetweeted_status_id() != 0) {
                retweetCount++;
                continue;
            }

            sortedResults.add(new DocumentComparable(p));
            count += 1;
        }
        if (filterRT) {
            logger.info("filter_rt count: {}", retweetCount);
        }

        List<Document> docs = Lists.newArrayList();

        int i = 1;
        int duplicateCount = 0;
        double rsvPrev = 0;
        for (DocumentComparable sortedResult : sortedResults) {
            if (i > n + 1)
                break;
            Document result = sortedResult.getDocument();
            double rsvCurr = result.rsv;
            if (Math.abs(rsvCurr - rsvPrev) > 0.0000001) {
                duplicateCount = 0;
            } else {
                duplicateCount++;
                rsvCurr = rsvCurr - 0.000001 / results.size() * duplicateCount;
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

    private IndexSearcher getSearcher() throws IOException {
        try {
            if (reader == null) {
                reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
                searcher = new IndexSearcher(reader);
                searcher.setSimilarity(SIMILARITY);
            } else if (live && !reader.isCurrent()) {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                    searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(SIMILARITY);
                }
            }
        } catch (IndexNotFoundException e) {
            logger.error(e.getMessage());
        }
        return searcher;
    }
}
