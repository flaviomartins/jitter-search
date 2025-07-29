package io.jitter.core.selection;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.ImmutableSortedSet;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.selection.methods.RankS;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.shards.ShardStats;
import io.jitter.core.shards.ShardStatsBuilder;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.twitter.manager.TwitterManager;
import io.jitter.core.utils.SearchUtils;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.LongPoint;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SelectionManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManager.class);

    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private final Analyzer analyzer;
    private final LMDirichletSimilarity similarity;
    private final QueryLikelihoodModel qlModel;

    private DirectoryReader indexReader;
    private IndexSearcher searcher;

    private final String collection;
    private final String indexPath;
    private Stopper stopper;
    private final float mu;
    private final String method;
    private final boolean live;

    private Map<String, ImmutableSortedSet<String>> topics;

    private ShardStatsBuilder shardStatsBuilder;
    private Map<String, String> reverseTopicMap;
    private ShardStats csiStats;
    private ShardStats shardStats;

    private ShardsManager shardsManager;
    private TwitterManager twitterManager;
    private boolean indexing;

    public SelectionManager(String collection, String indexPath, String stopwords, float mu, String method, boolean live, Map<String, Set<String>> topics) {
        this.collection = collection;
        this.indexPath = indexPath;
        this.mu = mu;
        this.method = method;
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
            shardStatsBuilder = new ShardStatsBuilder(indexReader, topics);
            reverseTopicMap = shardStatsBuilder.getReverseTopicMap();
            collectStats();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void collectStats() throws IOException {
        shardStatsBuilder.collectStats();
        csiStats = shardStatsBuilder.getCollectionsShardStats();
        shardStats = shardStatsBuilder.getTopicsShardStats();
    }

    @Override
    public void stop() throws Exception {
        if (indexReader != null) {
            indexReader.close();
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

    public float getMu() {
        return mu;
    }

    public Map<String, ImmutableSortedSet<String>> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, ImmutableSortedSet<String>> topics) {
        this.topics = topics;
    }

    public ShardStats getCsiStats() {
        return csiStats;
    }

    public ShardStats getShardStats() {
        return shardStats;
    }

    public ShardsManager getShardsManager() {
        return shardsManager;
    }

    public TwitterManager getTwitterManager() {
        return twitterManager;
    }

    public void setCsiStats(ShardStats csiStats) {
        this.csiStats = csiStats;
    }

    public void setShardStats(ShardStats shardStats) {
        this.shardStats = shardStats;
    }

    public void setShardsManager(ShardsManager shardsManager) {
        this.shardsManager = shardsManager;
    }

    public void setTwitterManager(TwitterManager twitterManager) {
        this.twitterManager = twitterManager;
    }

    public Map<String, Double> select(SelectionTopDocuments selectionTopDocuments, int limit, SelectionMethod selectionMethod, int maxCol, double minRanks, boolean normalize) {
        List<StatusDocument> topDocs = (List<StatusDocument>) selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        for (StatusDocument topDoc : topDocs) {
            topDoc.setShardIds(new String[]{topDoc.getScreen_name()});
        }
        Map<String, Double> rankedCollections = selectionMethod.rank(topDocs, csiStats);
        SortedMap<String, Double> ranking;
        if (normalize && shardsManager.getCollectionsShardStats() != null) {
            Map<String, Double> map = selectionMethod.normalize(rankedCollections, csiStats, shardsManager.getCollectionsShardStats());
            ranking = getSortedMap(map);
        } else {
            ranking = getSortedMap(rankedCollections);
        }

        return limit(selectionMethod, ranking, maxCol, minRanks);
    }

    public Map<String, Double> selectTopics(SelectionTopDocuments selectionTopDocuments, int limit, SelectionMethod selectionMethod, int maxCol, double minRanks, boolean normalize) {
        List<StatusDocument> topDocs = (List<StatusDocument>) selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        for (StatusDocument topDoc : topDocs) {
            if (reverseTopicMap.containsKey(topDoc.getScreen_name().toLowerCase(Locale.ROOT))) {
                String topic = reverseTopicMap.get(topDoc.getScreen_name().toLowerCase(Locale.ROOT)).toLowerCase(Locale.ROOT);
                topDoc.setShardIds(new String[]{topic});
            } else {
                logger.error("{} not mapped to a topic!", topDoc.getScreen_name());
            }
        }
        Map<String, Double> rankedTopics = selectionMethod.rank(topDocs, shardStats);
        SortedMap<String, Double> ranking;
        if (normalize && shardsManager.getTopicsShardStats() != null) {
            Map<String, Double> map = selectionMethod.normalize(rankedTopics, shardStats, shardsManager.getTopicsShardStats());
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

    public SelectionTopDocuments isearch(String query, String filterQuery, Query filter, int n, boolean filterRT) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        int[] ids;
        float[] scores;

        IndexSearcher indexSearcher = getIndexSearcher();
        CollectionStats collectionStats = getCollectionStats();
        Query q = new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer).parse(query);

        BooleanQuery.Builder b = new BooleanQuery.Builder();
        b.add(q, BooleanClause.Occur.MUST);

        if (!filterQuery.isEmpty()) {
            Query fq = new QueryParser(IndexStatuses.StatusField.TEXT.name, new WhitespaceAnalyzer()).parse(filterQuery);
            b.add(fq, BooleanClause.Occur.FILTER);
        }

        if (filter != null) {
            b.add(filter, BooleanClause.Occur.FILTER);
        }

        Query bQuery = b.build();

        final TopDocsCollector hitsCollector = TopScoreDocCollector.create(len, len);
        indexSearcher.search(bQuery, hitsCollector);

        totalHits = hitsCollector.getTotalHits();
        TopDocs topDocs = hitsCollector.topDocs(0, len);

        nDocsReturned = topDocs.scoreDocs.length;
        ids = new int[nDocsReturned];
        scores = new float[nDocsReturned];
        for (int i = 0; i < nDocsReturned; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            ids[i] = scoreDoc.doc;
            scores[i] = scoreDoc.score;
        }

        List<StatusDocument> docs = SearchUtils.getDocs(indexSearcher, analyzer, collectionStats, qlModel, topDocs, query, n, filterRT);

        int c_sel;
        if (live) {
            c_sel = totalHits;
        } else {
            Terms terms = MultiTerms.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
            TermsEnum termEnum = terms.iterator();
            final BytesRefBuilder bytes = new BytesRefBuilder();

            int totalDF = 0;
            Set<Term> queryTerms = new TreeSet<>();
            QueryVisitor termCollector = QueryVisitor.termCollector(queryTerms);
            q.visit(termCollector);
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

    public SelectionTopDocuments isearch(String query, String filterQuery, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, filterQuery, null, n, filterRT);
    }

    public SelectionTopDocuments isearch(String query, String filterQuery, int n) throws IOException, ParseException {
        return isearch(query, filterQuery, null, n, false);
    }

    public SelectionTopDocuments search(String query, String filterQuery, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        Query filter = LongPoint.newRangeQuery(IndexStatuses.StatusField.ID.name, 0L, maxId);
        return isearch(query, filterQuery, filter, n, filterRT);
    }

    public SelectionTopDocuments search(String query, String filterQuery, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        Query filter = LongPoint.newRangeQuery(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch);
        return isearch(query, filterQuery, filter, n, filterRT);
    }

    public SelectionTopDocuments search(String query, String filterQuery, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, filterQuery, n, filterRT);
    }

    public SelectionTopDocuments search(String query, String filterQuery, int n) throws IOException, ParseException {
        return isearch(query, filterQuery, n, false);
    }

    public void index() throws IOException {
        if (indexing)
            return;

        try {
            logger.info("selection indexing");
            indexing = true;
            twitterManager.index(collection, indexPath, analyzer);
        } catch (IOException e) {
            throw e;
        } finally {
            indexing = false;
        }
    }

    public void forceMerge() throws IOException {
        if (indexing)
            return;

        logger.info("Merging started!");
        long startTime = System.currentTimeMillis();
        Path indexPath = Paths.get(this.indexPath);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try (IndexWriter writer = new IndexWriter(dir, config)) {
            indexing = true;
            writer.forceMerge(1);
        } catch (IOException e) {
            throw e;
        } finally {
            dir.close();
            long endTime = System.currentTimeMillis();
            logger.info(String.format(Locale.ENGLISH, "Merging finished! Total time: %4dms", (endTime - startTime)));
            indexing = false;
        }
    }

    public TermStats[] getHighFreqTerms(int n) throws Exception {
        int numResults = n > MAX_TERMS_RESULTS ? MAX_TERMS_RESULTS : n;
        return HighFreqTerms.getHighFreqTerms(indexReader, numResults, IndexStatuses.StatusField.TEXT.name, new HighFreqTerms.DocFreqComparator());
    }

    private IndexSearcher getIndexSearcher() throws IOException {
        try {
            if (indexReader == null) {
                indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
                searcher = new IndexSearcher(indexReader);
                searcher.setSimilarity(similarity);
            } else if (live && !indexReader.isCurrent()) {
                DirectoryReader newReader = DirectoryReader.openIfChanged(indexReader);
                if (newReader != null) {
                    indexReader.close();
                    indexReader = newReader;
                    searcher = new IndexSearcher(indexReader);
                    searcher.setSimilarity(similarity);
                }
            }
        } catch (IndexNotFoundException e) {
            logger.error(e.getMessage());
        }
        return searcher;
    }

    public CollectionStats getCollectionStats() {
        return new IndexCollectionStats(indexReader, IndexStatuses.StatusField.TEXT.name);
    }

    public SelectionTopDocuments search(String query, String filterQuery, Optional<Long> maxId, int limit, boolean retweets, long[] epochs, boolean future) throws IOException, ParseException {
        SelectionTopDocuments selectResults;
        if (!future) {
            if (maxId.isPresent()) {
                selectResults = search(query, filterQuery, limit, !retweets, maxId.get());
            } else if (epochs[0] > 0 || epochs[1] > 0) {
                selectResults = search(query, filterQuery, limit, !retweets, epochs[0], epochs[1]);
            } else {
                selectResults = search(query, filterQuery, limit, !retweets);
            }
        } else {
            selectResults = search(query, filterQuery, limit, !retweets);
        }
        return selectResults;
    }

    public CsiSelection selection(String query, String filterQuery, Optional<Long> maxId, long[] epochs, int limit, boolean retweets, boolean future, String method, int maxCol, Double minRanks, boolean normalize, boolean topics) throws IOException, ParseException {
        return new CsiSelection(query, filterQuery, maxId, epochs, limit, retweets, future, method, maxCol, minRanks, normalize, topics).invoke();
    }

    public boolean isIndexing() {
        return indexing;
    }

    public class CsiSelection implements Selection {
        private final String query;
        private final String filterQuery;
        private final Optional<Long> maxId;
        private final long[] epochs;
        private final boolean topics;
        private final int limit;
        private final boolean retweets;
        private final boolean future;
        private final String method;
        private final int maxCol;
        private final double minRanks;
        private final boolean normalize;
        private SelectionTopDocuments results;
        private Map<String, Double> collections;

        public CsiSelection(String query, String filterQuery, Optional<Long> maxId, long[] epochs, int limit,
                            boolean retweets, boolean future,
                            String method, int maxCol, double minRanks, boolean normalize, boolean topics) {
            this.query = query;
            this.filterQuery = filterQuery;
            this.maxId = maxId;
            this.limit = limit;
            this.retweets = retweets;
            this.future = future;
            this.method = method;
            this.maxCol = maxCol;
            this.minRanks = minRanks;
            this.normalize = normalize;
            this.epochs = epochs;
            this.topics = topics;
        }

        @Override
        public SelectionTopDocuments getResults() {
            return results;
        }

        @Override
        public Map<String, Double> getCollections() {
            return collections;
        }

        public CsiSelection invoke() throws IOException, ParseException {
            results = search(query, filterQuery, maxId, limit, retweets, epochs, future);
            SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
            if (topics) {
                collections = selectTopics(results, limit, selectionMethod, maxCol, minRanks, normalize);
            } else {
                collections = select(results, limit, selectionMethod, maxCol, minRanks, normalize);
            }
            return this;
        }
    }
}
