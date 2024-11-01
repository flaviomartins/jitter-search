package io.jitter.core.shards;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.api.search.AbstractDocument;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.twitter.manager.TwitterManager;
import io.jitter.core.utils.SearchUtils;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ShardsManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(ShardsManager.class);

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
    private final boolean removeDuplicates;
    private final boolean live;

    private Map<String, ImmutableSortedSet<String>> topics;

    private ShardStatsBuilder shardStatsBuilder;
    private Map<String, String> reverseTopicMap;
    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;

    private TwitterManager twitterManager;
    private TailyManager tailyManager;
    private boolean indexing;

    public ShardsManager(String collection, String indexPath, String stopwords, float mu, String method, boolean removeDuplicates, boolean live, Map<String, Set<String>> topics) {
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
            analyzer = new TweetAnalyzer();
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
        collectionsShardStats = shardStatsBuilder.getCollectionsShardStats();
        topicsShardStats = shardStatsBuilder.getTopicsShardStats();
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

    private SelectionTopDocuments filter(Query query, Set<String> selectedSources, SelectionTopDocuments selectResults) throws IOException {
        List<StatusDocument> topDocs = (List<StatusDocument>) selectResults.scoreDocs;
        List<StatusDocument> shardedDocs = new ArrayList<>();
        for (StatusDocument topDoc : topDocs) {
            topDoc.setShardIds(new String[]{topDoc.getScreen_name()});
        }
        if (selectedSources != null && !selectedSources.isEmpty()) {
            for (StatusDocument doc : topDocs) {
                ImmutableSortedSet<String> shardIds = new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                        .addAll(ImmutableSet.copyOf(doc.getShardIds()))
                        .build();
                if (Sets.intersection(selectedSources, shardIds).size() > 0) {
                    shardedDocs.add(doc);
                }
            }
        } else {
            shardedDocs.addAll(topDocs);
        }

        int c_r;
        if (live) {
            c_r = selectResults.totalHits;
        } else {
            int totalDF = 0;
            Set<Term> queryTerms = new TreeSet<>();
            query.createWeight(searcher, false).extractTerms(queryTerms);
            for (Term term : queryTerms) {
                String text = term.text();
                if (text.isEmpty())
                    continue;

                if (selectedSources != null) {
                    for (String selectedSource : selectedSources) {
                        totalDF += tailyManager.getDF(selectedSource, text);
                    }
                } else {
                    for (String source : reverseTopicMap.keySet()) {
                        totalDF += tailyManager.getDF(source, text);
                    }
                }
            }
            c_r = totalDF;
        }

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(shardedDocs.size(), shardedDocs);
        selectionTopDocuments.setC_r(c_r);
        return selectionTopDocuments;
    }

    private SelectionTopDocuments filterTopics(Query query, Set<String> selectedTopics, SelectionTopDocuments selectResults) throws IOException {
        List<StatusDocument> topDocs = (List<StatusDocument>) selectResults.scoreDocs;
        List<StatusDocument> shardedDocs = new ArrayList<>();
        for (StatusDocument topDoc : topDocs) {
            topDoc.setShardIds(new String[]{topDoc.getScreen_name()});
        }
        if (selectedTopics != null && !selectedTopics.isEmpty()) {
            for (StatusDocument doc : topDocs) {
                ImmutableSortedSet<String> shardIds = new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                        .addAll(ImmutableSet.copyOf(doc.getShardIds()))
                        .build();
                for (String selectedTopic : selectedTopics) {
                    if (topics.get(selectedTopic) != null && Sets.intersection(topics.get(selectedTopic), shardIds).size() > 0) {
                        shardedDocs.add(doc);
                    }
                }
            }
        } else {
            shardedDocs.addAll(topDocs);
        }

        int c_r;
        if (live) {
            c_r = selectResults.totalHits;
        } else {
            int totalDF = 0;
            Set<Term> queryTerms = new TreeSet<>();
            query.createWeight(searcher, false).extractTerms(queryTerms);
            for (Term term : queryTerms) {
                String text = term.text();
                if (text.isEmpty())
                    continue;

                if (selectedTopics != null) {
                    for (String selectedTopic : selectedTopics) {
                        totalDF += tailyManager.getTopicsDF(selectedTopic, text);
                    }
                } else {
                    for (String topic : topics.keySet()) {
                        totalDF += tailyManager.getTopicsDF(topic, text);
                    }
                }
            }
            c_r = totalDF;
        }

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(shardedDocs.size(), shardedDocs);
        selectionTopDocuments.setC_r(c_r);
        return selectionTopDocuments;
    }

    public SelectionTopDocuments limit(SelectionTopDocuments selectionTopDocuments, int limit) {
        selectionTopDocuments.scoreDocs = selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        return selectionTopDocuments;
    }

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, String filterQuery, Filter filter, int n, boolean filterRT) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        float maxScore;
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

        Query bQuery = b.build();

        final TopDocsCollector hitsCollector = TopScoreDocCollector.create(len, null);
        indexSearcher.search(bQuery, filter, hitsCollector);

        totalHits = hitsCollector.getTotalHits();
        TopDocs topDocs;
        if (live) {
            topDocs = hitsCollector.topDocs(0, len);
        } else {
            topDocs = hitsCollector.topDocs(0, indexReader.numDocs());
        }

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

        List<StatusDocument> docs = SearchUtils.getDocs(indexSearcher, analyzer, collectionStats, qlModel, topDocs, query, n, filterRT);

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(totalHits, docs);

        if (topics) {
            return limit(filterTopics(q, collections, selectionTopDocuments), n);
        } else {
            return limit(filter(q, collections, selectionTopDocuments), n);
        }
    }

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, String filterQuery, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(topics, collections, query, filterQuery, null, n, filterRT);
    }

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, String filterQuery, int n) throws IOException, ParseException {
        return isearch(topics, collections, query, filterQuery, null, n, false);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, String filterQuery, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        return isearch(topics, collections, query, filterQuery, filter, n, filterRT);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, String filterQuery, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        return isearch(topics, collections, query, filterQuery, filter, n, filterRT);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, String filterQuery, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(topics, collections, query, filterQuery, n, filterRT);
    }

    public SelectionTopDocuments search(boolean topics, Set<String> collections, String query, String filterQuery, int n) throws IOException, ParseException {
        return isearch(topics, collections, query, filterQuery, n, false);
    }

    public void index() throws IOException {
        if (indexing)
            return;

        try {
            logger.info("shards indexing");
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

    public SelectionTopDocuments search(Optional<Long> maxId, long[] epochs, boolean retweets, boolean future, int limit, boolean topics, String query, String filterQuery, Set<String> selected) throws IOException, ParseException {
        SelectionTopDocuments shardResults;
        if (!future) {
            if (maxId.isPresent()) {
                shardResults = search(topics, selected, query, filterQuery, limit, !retweets, maxId.get());
            } else if (epochs[0] > 0 || epochs[1] > 0) {
                shardResults = search(topics, selected, query, filterQuery, limit, !retweets, epochs[0], epochs[1]);
            } else {
                shardResults = search(topics, selected, query, filterQuery, limit, !retweets);
            }
        } else {
            shardResults = search(topics, selected, query, filterQuery, limit, !retweets);
        }
        return shardResults;
    }

    public boolean isIndexing() {
        return indexing;
    }
}
