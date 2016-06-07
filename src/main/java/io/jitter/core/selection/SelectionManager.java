package io.jitter.core.selection;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.ImmutableSortedSet;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.selection.methods.RankS;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.shards.ShardStatsBuilder;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.shards.ShardStats;
import io.jitter.core.twitter.manager.TwitterManager;
import io.jitter.core.utils.SearchUtils;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.*;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SelectionManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManager.class);

    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private final Analyzer analyzer;
    private final LMDirichletSimilarity similarity;
    private final QueryLikelihoodModel qlModel;

    private DirectoryReader reader;
    private IndexSearcher searcher;

    private final String collection;
    private final String indexPath;
    private Stopper stopper;
    private final int mu;
    private final String method;
    private final boolean removeDuplicates;
    private final boolean live;

    private Map<String, ImmutableSortedSet<String>> topics;

    private ShardStatsBuilder shardStatsBuilder;
    private Map<String, String> reverseTopicMap;
    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;

    private ShardsManager shardsManager;
    private TwitterManager twitterManager;

    public SelectionManager(String collection, String indexPath, String stopwords, int mu, String method, boolean removeDuplicates, boolean live, Map<String, Set<String>> topics) {
        this.collection = collection;
        this.indexPath = indexPath;
        this.mu = mu;
        this.method = method;
        this.removeDuplicates = removeDuplicates;
        this.live = live;

        TreeMap<String, ImmutableSortedSet<String>> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : topics.entrySet()) {
            treeMap.put(entry.getKey(), new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER).addAll(entry.getValue()).build());
        }
        this.topics = treeMap;

        similarity = new LMDirichletSimilarity(mu);
        qlModel = new QueryLikelihoodModel(mu);

        if (!stopwords.isEmpty()) {
            stopper = new Stopper(stopwords);
        }
        if (stopper == null || stopper.asSet().isEmpty()) {
            analyzer = new TweetAnalyzer(CharArraySet.EMPTY_SET);
        } else {
            CharArraySet charArraySet = new CharArraySet(stopper.asSet(), true);
            analyzer = new TweetAnalyzer(charArraySet);
        }
    }

    @Override
    public void start() throws Exception {
        try {
            searcher = getIndexSearcher();
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

    public Stopper getStopper() {
        return stopper;
    }

    public int getMu() {
        return mu;
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

    public ShardsManager getShardsManager() {
        return shardsManager;
    }

    public TwitterManager getTwitterManager() {
        return twitterManager;
    }

    public void setCollectionsShardStats(ShardStats collectionsShardStats) {
        this.collectionsShardStats = collectionsShardStats;
    }

    public void setTopicsShardStats(ShardStats topicsShardStats) {
        this.topicsShardStats = topicsShardStats;
    }

    public void setShardsManager(ShardsManager shardsManager) {
        this.shardsManager = shardsManager;
    }

    public void setTwitterManager(TwitterManager twitterManager) {
        this.twitterManager = twitterManager;
    }

    public Map<String, Double> select(SelectionTopDocuments selectionTopDocuments, int limit, SelectionMethod selectionMethod, int maxCol, double minRanks, boolean normalize) {
        List<Document> topDocs = selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        Map<String, Double> rankedCollections = selectionMethod.rank(topDocs);
        SortedMap<String, Double> ranking;
        if (normalize && shardsManager.getCollectionsShardStats() != null) {
            Map<String, Double> map = selectionMethod.normalize(rankedCollections, collectionsShardStats, shardsManager.getCollectionsShardStats());
            ranking = getSortedMap(map);
        } else {
            ranking = getSortedMap(rankedCollections);
        }

        return limit(selectionMethod, ranking, maxCol, minRanks);
    }

    public Map<String, Double> selectTopics(SelectionTopDocuments selectionTopDocuments, int limit, SelectionMethod selectionMethod, int maxCol, double minRanks, boolean normalize) {
        List<Document> topDocs = selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        Map<String, Double> rankedCollections = selectionMethod.rank(topDocs);
        Map<String, Double> rankedTopics = new HashMap<>();

        for (String col : rankedCollections.keySet()) {
            if (reverseTopicMap.containsKey(col.toLowerCase(Locale.ROOT))) {
                String topic = reverseTopicMap.get(col.toLowerCase(Locale.ROOT)).toLowerCase(Locale.ROOT);
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

        SortedMap<String, Double> ranking;
        if (normalize && shardsManager.getTopicsShardStats() != null) {
            Map<String, Double> map = selectionMethod.normalize(rankedTopics, topicsShardStats, shardsManager.getTopicsShardStats());
            ranking = getSortedMap(map);
        } else {
            ranking = getSortedMap(rankedTopics);
        }

        return limit(selectionMethod, ranking, maxCol, minRanks);
    }

    private SortedMap<String, Double> getSortedMap(Map<String, Double> map) {
        SelectionComparator comparator = new SelectionComparator(map);
        TreeMap<String, Double> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(map);
        return sortedMap;
    }

    private Map<String,Double> limit(SelectionMethod selectionMethod, SortedMap<String, Double> ranking, int maxCol, double minRanks) {
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

    public SelectionTopDocuments reScoreSelected(Iterable<Map.Entry<String, Double>> selectedTopics, List<Document> selectResults) {
        List<Document> docs = new ArrayList<>();
        for (Document doc : selectResults) {
            Document updatedDocument = new Document(doc);
            for (Map.Entry<String, Double> selectedTopic : selectedTopics) {
                if (topics.get(selectedTopic.getKey()) != null && topics.get(selectedTopic.getKey()).contains(doc.getScreen_name())) {
                    double newRsv = selectedTopic.getValue() * doc.getRsv();
                    updatedDocument.setRsv(newRsv);
                    docs.add(updatedDocument);
                }
            }
        }
        return new SelectionTopDocuments(docs.size(), docs);
    }

    public SelectionTopDocuments isearch(String query, Filter filter, int n, boolean filterRT) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        float maxScore;
        int[] ids;
        float[] scores;

        IndexSearcher indexSearcher = getIndexSearcher();
        Query q = new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer).parse(query);

        final TopDocsCollector topCollector = TopScoreDocCollector.create(len, true);
        indexSearcher.search(q, filter, topCollector);

        totalHits = topCollector.getTotalHits();
        TopDocs topDocs = topCollector.topDocs(0, len);

        //noinspection UnusedAssignment
        maxScore = totalHits > 0 ? topDocs.getMaxScore() : 0.0f;
        nDocsReturned = topDocs.scoreDocs.length;
        ids = new int[nDocsReturned];
        scores = new float[nDocsReturned];
        for (int i = 0; i < nDocsReturned; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            ids[i] = scoreDoc.doc;
            scores[i] = scoreDoc.score;
        }

        List<Document> docs = SearchUtils.getDocs(indexSearcher, qlModel, topDocs, query, n, filterRT);
        if (filterRT) {
            logger.info("filter_rt count: {}", nDocsReturned - docs.size());
        }

        int c_sel;
        if (live) {
            c_sel = totalHits;
        } else {
            Terms terms = MultiFields.getTerms(reader, IndexStatuses.StatusField.TEXT.name);
            TermsEnum termEnum = terms.iterator(null);
            final BytesRefBuilder bytes = new BytesRefBuilder();

            int totalDF = 0;
            Set<Term> queryTerms = new TreeSet<>();
            q.extractTerms(queryTerms);
            for (Term term : queryTerms) {
                String text = term.text();
                if (text.isEmpty())
                    continue;
                bytes.copyChars(text);
                termEnum.seekExact(bytes.toBytesRef());
                totalDF += termEnum.docFreq();
            }
            c_sel = totalDF;
        }

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(totalHits, docs);
        selectionTopDocuments.setC_sel(c_sel);
        return selectionTopDocuments;
    }

    public SelectionTopDocuments isearch(String query, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, null, n, filterRT);
    }

    public SelectionTopDocuments isearch(String query, int n) throws IOException, ParseException {
        return isearch(query, null, n, false);
    }

    public SelectionTopDocuments search(String query, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        return isearch(query, filter, n, filterRT);
    }

    public SelectionTopDocuments search(String query, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        return isearch(query, filter, n, filterRT);
    }

    public SelectionTopDocuments search(String query, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, n, filterRT);
    }

    public SelectionTopDocuments search(String query, int n) throws IOException, ParseException {
        return isearch(query, n, false);
    }

    public void index() throws IOException {
        logger.info("selection indexing");
        twitterManager.index(collection, indexPath, analyzer, removeDuplicates);
    }

    public void forceMerge() throws IOException {
        logger.info("Merging started!");
        long startTime = System.currentTimeMillis();
        File indexPath = new File(this.indexPath);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try (IndexWriter writer = new IndexWriter(dir, config)) {
            writer.forceMerge(1);
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        } finally {
            dir.close();
            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "Merging finished! Total time: %4dms", (endTime - startTime)));

        }
    }

    public TermStats[] getHighFreqTerms(int n) throws Exception {
        int numResults = n > MAX_TERMS_RESULTS ? MAX_TERMS_RESULTS : n;
        return HighFreqTerms.getHighFreqTerms(reader, numResults, IndexStatuses.StatusField.TEXT.name, new HighFreqTerms.DocFreqComparator());
    }

    private IndexSearcher getIndexSearcher() throws IOException {
        try {
            if (reader == null) {
                reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
                searcher = new IndexSearcher(reader);
                searcher.setSimilarity(similarity);
            } else if (live && !reader.isCurrent()) {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                    searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(similarity);
                }
            }
        } catch (IndexNotFoundException e) {
            logger.error(e.getMessage());
        }
        return searcher;
    }

    public CollectionStats getCollectionStats() {
        return new IndexCollectionStats(reader, IndexStatuses.StatusField.TEXT.name);
    }

    public SelectionTopDocuments search(Optional<Long> maxId, Optional<String> epoch, int limit, boolean retweets, boolean future, String query, long[] epochs) throws IOException, ParseException {
        SelectionTopDocuments selectResults;
        if (!future) {
            if (maxId.isPresent()) {
                selectResults = search(query, limit, !retweets, maxId.get());
            } else if (epoch.isPresent()) {
                selectResults = search(query, limit, !retweets, epochs[0], epochs[1]);
            } else {
                selectResults = search(query, limit, !retweets);
            }
        } else {
            selectResults = search(query, limit, !retweets);
        }
        return selectResults;
    }

    public Map<String, Double> select(int limit, boolean topics, int maxCol, Double minRanks, boolean normalize, SelectionTopDocuments selectResults, SelectionMethod selectionMethod) {
        Map<String, Double> selected;
        if (!topics) {
            selected = select(selectResults, limit, selectionMethod, maxCol, minRanks, normalize);
        } else {
            selected = selectTopics(selectResults, limit, selectionMethod, maxCol, minRanks, normalize);
        }
        return selected;
    }

    public CsiSelection selection(Optional<Long> maxId, Optional<String> epoch, int limit, boolean retweets, boolean future, String method, int maxCol, Double minRanks, boolean normalize, String query, long[] epochs) throws IOException, ParseException {
        return new CsiSelection(maxId, epoch, limit, retweets, future, method, maxCol, minRanks, normalize, query, epochs).invoke();
    }

    public class CsiSelection implements Selection {
        private final Optional<Long> maxId;
        private final Optional<String> epoch;
        private final int limit;
        private final boolean retweets;
        private final boolean future;
        private final String method;
        private final int maxCol;
        private final double minRanks;
        private final boolean normalize;
        private final String query;
        private final long[] epochs;
        private SelectionTopDocuments results;
        private Map<String, Double> sources;
        private Map<String, Double> topics;

        public CsiSelection(Optional<Long> maxId, Optional<String> epoch, int limit, boolean retweets, boolean future, String method, int maxCol, double minRanks, boolean normalize, String query, long[] epochs) {
            this.maxId = maxId;
            this.epoch = epoch;
            this.limit = limit;
            this.retweets = retweets;
            this.future = future;
            this.method = method;
            this.maxCol = maxCol;
            this.minRanks = minRanks;
            this.normalize = normalize;
            this.query = query;
            this.epochs = epochs;
        }

        @Override
        public SelectionTopDocuments getResults() {
            return results;
        }

        @Override
        public Map<String, Double> getSources() {
            return sources;
        }

        @Override
        public Map<String, Double> getTopics() {
            return topics;
        }

        public CsiSelection invoke() throws IOException, ParseException {
            results = search(maxId, epoch, limit, retweets, future, query, epochs);
            SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
            sources = select(results, limit, selectionMethod, maxCol, minRanks, normalize);
            topics = selectTopics(results, limit, selectionMethod, maxCol, minRanks, normalize);
            return this;
        }
    }
}
