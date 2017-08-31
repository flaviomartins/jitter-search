package io.jitter.core.wikipedia;

import cc.twittertools.util.QueryLikelihoodModel;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.api.wikipedia.WikipediaDocument;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionComparator;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.selection.methods.RankS;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.shards.ShardStats;
import io.jitter.core.utils.Stopper;
import io.jitter.core.utils.WikipediaSearchUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.*;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRefBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class WikipediaSelectionManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(WikipediaSelectionManager.class);

    public static final String ID_FIELD = "id";
    public static final String TITLE_FIELD = "titleTokenized";
    public static final String TEXT_FIELD = "body";
    public static final String DATE_FIELD = "date";
    public static final String CATEGORIES_FIELD = "categories";
    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private final Analyzer analyzer;
    private final LMDirichletSimilarity similarity;
    private final QueryLikelihoodModel qlModel;

    private final String indexPath;
    private Stopper stopper;
    private final float mu;
    private final String method;
    private final boolean live;
    private final PetscanCsvCategoryMapper categoryMapper;

    private DirectoryReader indexReader;
    private IndexSearcher searcher;
    private TaxonomyReader taxoReader;
    private FacetsConfig facetsConfig = new FacetsConfig();

    private WikipediaShardStatsBuilder wikipediaShardStatsBuilder;
    private ShardStats csiStats;
    private ShardStats shardStats;
    private WikipediaShardsManager shardsManager;

    public WikipediaSelectionManager(String indexPath, String stopwords, float mu, String method, String cat2Topic, boolean live) throws IOException {
        this.indexPath = indexPath;
        this.mu = mu;
        this.method = method;
        this.live = live;
        categoryMapper = new PetscanCsvCategoryMapper(new File(cat2Topic));

        similarity = new LMDirichletSimilarity(mu);
        qlModel = new QueryLikelihoodModel(mu);

        if (!stopwords.isEmpty()) {
            stopper = new Stopper(stopwords);
        }
        if (stopper == null || stopper.asSet().isEmpty()) {
            analyzer = new EnglishAnalyzer();
        } else {
            CharArraySet charArraySet = new CharArraySet(stopper.asSet(), true);
            analyzer = new EnglishAnalyzer(charArraySet);
        }
    }

    @Override
    public void start() throws Exception {
        try {
            searcher = getIndexSearcher();
            wikipediaShardStatsBuilder = new WikipediaShardStatsBuilder(indexReader, categoryMapper);
            collectStats();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void collectStats() throws IOException {
        wikipediaShardStatsBuilder.collectStats();
        csiStats = wikipediaShardStatsBuilder.getCollectionsShardStats();
        shardStats = wikipediaShardStatsBuilder.getTopicsShardStats();
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

    public ShardStats getCsiStats() {
        return csiStats;
    }

    public ShardStats getShardStats() {
        return shardStats;
    }

    public void setCsiStats(ShardStats csiStats) {
        this.csiStats = csiStats;
    }

    public void setShardStats(ShardStats shardStats) {
        this.shardStats = shardStats;
    }

    public void setShardsManager(WikipediaShardsManager shardsManager) {
        this.shardsManager = shardsManager;
    }

    public Map<String, Double> select(SelectionTopDocuments selectionTopDocuments, int limit, SelectionMethod selectionMethod, int maxCol, double minRanks, boolean normalize) {
        List<WikipediaDocument> topDocs = (List<WikipediaDocument>) selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        for (WikipediaDocument topDoc : topDocs) {
            topDoc.setShardIds(topDoc.getCategories());
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
        List<WikipediaDocument> topDocs = (List<WikipediaDocument>) selectionTopDocuments.scoreDocs.subList(0, Math.min(limit, selectionTopDocuments.scoreDocs.size()));
        for (WikipediaDocument topDoc : topDocs) {
            topDoc.setShardIds(topDoc.getTopics());
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

    public SelectionTopDocuments isearch(String query, Filter filter, int n, boolean full) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        float maxScore;
        int[] ids;
        float[] scores;

        IndexSearcher indexSearcher = getIndexSearcher();
        CollectionStats collectionStats = getCollectionStats();
        Query q = new QueryParser(TEXT_FIELD, analyzer).parse(query);

        final TopDocsCollector hitsCollector = TopScoreDocCollector.create(len, null);
        final FacetsCollector fc = new FacetsCollector();
        indexSearcher.search(q, filter, MultiCollector.wrap(hitsCollector, fc));

        // Retrieve results
        List<FacetResult> results = new ArrayList<>();
        Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, fc);
        results.add(facets.getTopChildren(10, "Categories"));

        totalHits = hitsCollector.getTotalHits();
        TopDocs topDocs = hitsCollector.topDocs(0, len);

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

        // Compute real QL scores even when live to build termVectors
        List<WikipediaDocument> docs = WikipediaSearchUtils.getDocs(indexSearcher, collectionStats, qlModel, topDocs, query, n);

        for (WikipediaDocument doc : docs) {
            HashSet<Object> docTopics = new HashSet<>();
            for (String cat : doc.getCategories()) {
                Set<String> catTopics = categoryMapper.getMap().get(cat);
                if (catTopics != null) {
                    docTopics.addAll(catTopics);
                }
            }
            doc.setTopics(docTopics.toArray(new String[docTopics.size()]));

            if (!full) {
                doc.setText(StringUtils.abbreviate(doc.getText(), 500));
            }
        }

        int c_sel;
        if (live) {
            c_sel = totalHits;
        } else {
            Terms terms = MultiFields.getTerms(indexReader, TEXT_FIELD);
            TermsEnum termEnum = terms.iterator();
            final BytesRefBuilder bytes = new BytesRefBuilder();

            int totalDF = 0;
            Set<Term> queryTerms = new TreeSet<>();
            q.createWeight(searcher, false).extractTerms(queryTerms);
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

    public TermStats[] getHighFreqTerms(int n) throws Exception {
        int numResults = n > MAX_TERMS_RESULTS ? MAX_TERMS_RESULTS : n;
        return HighFreqTerms.getHighFreqTerms(indexReader, numResults, TEXT_FIELD, new HighFreqTerms.DocFreqComparator());
    }

    private IndexSearcher getIndexSearcher() throws IOException {
        try {
            if (taxoReader == null) {
                taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(Paths.get(indexPath, "facets")));
            } else if (live) {
                taxoReader = DirectoryTaxonomyReader.openIfChanged(taxoReader);
            }

            if (indexReader == null) {
                indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath, "index")));
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
        return new IndexCollectionStats(indexReader, TEXT_FIELD);
    }

    public SelectionTopDocuments search(String query, int limit, boolean full) throws IOException, ParseException {
        return isearch(query, null, limit, full);
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

    public CsiSelection selection(int limit, String method, int maxCol, Double minRanks, boolean normalize,
                                  String query, boolean full, boolean topics) throws IOException, ParseException {
        return new CsiSelection(limit, method, maxCol, minRanks, normalize, query, full, topics).invoke();
    }

    public class CsiSelection implements Selection {
        private final int limit;
        private final String method;
        private final int maxCol;
        private final double minRanks;
        private final boolean normalize;
        private final String query;
        private final boolean full;
        private final boolean topics;
        private SelectionTopDocuments results;
        private Map<String, Double> collections;

        public CsiSelection(int limit, String method, int maxCol, double minRanks, boolean normalize, String query,
                            boolean full, boolean topics) {
            this.limit = limit;
            this.method = method;
            this.maxCol = maxCol;
            this.minRanks = minRanks;
            this.normalize = normalize;
            this.query = query;
            this.full = full;
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
            results = search(query, limit, full);
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
