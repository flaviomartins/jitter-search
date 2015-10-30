package io.jitter.core.selection;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.search.Document;
import io.jitter.core.probabilitydistributions.LocalExponentialDistribution;
import io.jitter.core.rerank.RecencyReranker;
import io.jitter.core.search.DocumentComparable;
import io.jitter.core.selection.methods.RankS;
import io.jitter.core.similarities.IDFSimilarity;
import io.jitter.core.twitter.manager.TwitterManager;
import io.jitter.core.utils.TimeUtils;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.*;
import io.jitter.core.selection.methods.SelectionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SelectionManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManager.class);

    public static final double DAY = 60.0 * 60.0 * 24.0;

    public static final int MAX_RESULTS = 10000;

    private static final QueryParser QUERY_PARSER =
            new QueryParser(IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);
    public static final IDFSimilarity SIMILARITY = new IDFSimilarity();

    private DirectoryReader reader;
    private IndexSearcher searcher;

    private final String indexPath;
    private final String method;
    private final boolean removeDuplicates;
    private final boolean live;

    private Map<String, ImmutableSortedSet<String>> topics;
    private final Map<String, String> reverseTopicMap;

    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;
    private TwitterManager twitterManager;

    public SelectionManager(String indexPath, String method, boolean removeDuplicates, boolean live, Map<String, Set<String>> topics) {
        this.indexPath = indexPath;
        this.method = method;
        this.removeDuplicates = removeDuplicates;
        this.live = live;

        TreeMap<String, ImmutableSortedSet<String>> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : topics.entrySet()) {
            treeMap.put(entry.getKey(), new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER).addAll(entry.getValue()).build());
        }

        Map<String, String> reverseMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            for (String col : entry.getValue()) {
                reverseMap.put(col.toLowerCase(), topic.toLowerCase());
            }
        }

        this.topics = treeMap;
        this.reverseTopicMap = reverseMap;
    }

    @Override
    public void start() throws Exception {
        try {
            searcher = getSearcher();
            collectStats();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void collectStats() throws IOException {
        Map<String, Integer> collectionsSizes = new HashMap<>();
        Map<String, Integer> topicsSizes = new HashMap<>();

        Terms terms = MultiFields.getTerms(reader, IndexStatuses.StatusField.SCREEN_NAME.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            int docFreq = termEnum.docFreq();
            if (term.isEmpty()) {
                continue;
            }

            String collection = term.toLowerCase();

            if (reverseTopicMap.keySet().contains(collection)) {
                collectionsSizes.put(collection, docFreq);
                logger.info("SCAN collection: " + collection + " " + docFreq);
            } else {
                logger.warn("SCAN collection: " + collection + " " + docFreq);
            }

            termCnt++;
        }

        logger.info("SCAN total collections: " + termCnt);

        for (String topic : topics.keySet()) {
            int docFreq = 0;
            for (String collection : topics.get(topic)) {
                Integer sz = collectionsSizes.get(collection);
                if (sz != null)
                    docFreq += sz;
            }

            topicsSizes.put(topic, docFreq);
            logger.info("SCAN topics: " + topic + " " + docFreq);
        }

        collectionsShardStats = new ShardStats(collectionsSizes);
        topicsShardStats = new ShardStats(topicsSizes);

        logger.info("SCAN total docs: " + collectionsShardStats.getTotalDocs() + " - " + topicsShardStats.getTotalDocs());
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

    public SortedMap<String, Double> getRanked(SelectionMethod selectionMethod, List<Document> results, boolean normalize) {
        Map<String, Double> rankedCollections = selectionMethod.rank(results);
        if (normalize && collectionsShardStats != null) {
            Map<String, Double> map = selectionMethod.normalize(rankedCollections, collectionsShardStats);
            return getSortedMap(map);
        } else {
            return getSortedMap(rankedCollections);
        }
    }

    public SortedMap<String, Double> getRankedTopics(SelectionMethod selectionMethod, List<Document> results, boolean normalize) {
        Map<String, Double> rankedCollections = selectionMethod.rank(results);
        Map<String, Double> rankedTopics = new HashMap<>();

        for (String col : rankedCollections.keySet()) {
            if (reverseTopicMap.containsKey(col.toLowerCase())) {
                String topic = reverseTopicMap.get(col.toLowerCase()).toLowerCase();
                double cur = 0;

                if (rankedTopics.containsKey(topic))
                    cur = rankedTopics.get(topic);
                else
                    rankedTopics.put(topic, 0d);

                double sum = cur + rankedCollections.get(col);
                rankedTopics.put(topic, sum);
            } else {
                logger.warn("{} not mapped to a topic!", col);
            }
        }

        if (normalize && topicsShardStats != null) {
            Map<String, Double> map = selectionMethod.normalize(rankedTopics, topicsShardStats);
            return getSortedMap(map);
        } else {
            return getSortedMap(rankedTopics);
        }
    }

    private SortedMap<String, Double> getSortedMap(Map<String, Double> map) {
        SelectionComparator comparator = new SelectionComparator(map);
        TreeMap<String, Double> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(map);
        return sortedMap;
    }

    public List<Document> filterTopic(String selectedTopic, List<Document> selectResults) {
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults) {
            if (topics.get(selectedTopic) != null && topics.get(selectedTopic).contains(doc.getScreen_name())) {
                results.add(doc);
            }
        }
        return results;
    }

    public List<Document> filterCollections(Iterable<String> selectedSources, List<Document> selectResults) {
        HashSet<String> collections = Sets.newHashSet(selectedSources);
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults) {
            if (collections.contains(doc.getScreen_name())) {
                results.add(doc);
            }
        }
        return results;
    }

    public List<Document> filterTopics(Iterable<String> selectedTopics, List<Document> selectResults) {
        List<Document> results = new ArrayList<>();
        for (Document doc : selectResults) {
            for (String selectedTopic: selectedTopics) {
                if (topics.get(selectedTopic) != null && topics.get(selectedTopic).contains(doc.getScreen_name())) {
                    results.add(doc);
                }
            }
        }
        return results;
    }

    public Map<String,Double> limit(SelectionMethod selectionMethod, Map<String, Double> ranking, int maxCol, double minRanks) {
        String methodName = selectionMethod.getClass().getSimpleName();
        Map<String, Double> map = new LinkedHashMap<>();
        // rankS has its own limit mechanism
        if (RankS.class.getSimpleName().equals(methodName)) {
            for (Map.Entry<String, Double> entry : ranking.entrySet()) {
                if (entry.getValue() < minRanks)
                    break;
                map.put(entry.getKey(), entry.getValue());
            }
        } else { // hard limit
            int i = 0;
            for (Map.Entry<String, Double> entry : ranking.entrySet()) {
                i++;
                if (i > maxCol)
                    break;
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    public List<Document> reScoreSelected(Iterable<Map.Entry<String, Double>> selectedTopics, List<Document> selectResults) {
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
        return sortResults(results, false);
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

    private List<Document> getSorted(TopDocs rs, boolean filterRT) throws IOException {
        List<Document> results = getResults(rs);
        return sortResults(results, filterRT);
    }

    private List<Document> sortResults(List<Document> results, boolean filterRT) {
        // get the query epoch
        double queryEpoch = System.currentTimeMillis() / 1000L;
        // extract raw epochs from results
        List<Double> rawEpochs = TimeUtils.extractEpochsFromResults(results);
        // groom our hit times wrt to query time
        List<Double> scaledEpochs = TimeUtils.adjustEpochsToLandmark(rawEpochs, queryEpoch, DAY);

        RecencyReranker reranker = new RecencyReranker(results, new LocalExponentialDistribution(0.01), scaledEpochs);
        results = reranker.getReranked();

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
        int duplicateCount = 0;
        double rsvPrev = 0;
        for (DocumentComparable sortedResult : sortedResults) {
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
