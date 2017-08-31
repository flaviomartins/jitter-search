package io.jitter.core.wikipedia;

import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.api.wikipedia.WikipediaDocument;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.shards.ShardStats;
import io.jitter.core.shards.ShardStatsBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class WikipediaShardsManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(WikipediaShardsManager.class);

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
    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;

    public WikipediaShardsManager(String indexPath, String stopwords, float mu, String method, String cat2Topic, boolean live) throws IOException {
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
        collectionsShardStats = wikipediaShardStatsBuilder.getCollectionsShardStats();
        topicsShardStats = wikipediaShardStatsBuilder.getTopicsShardStats();
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

    public ShardStats getCollectionsShardStats() {
        return collectionsShardStats;
    }

    public ShardStats getTopicsShardStats() {
        return topicsShardStats;
    }

    public void setCollectionsShardStats(ShardStats collectionsShardStats) {
        this.collectionsShardStats = collectionsShardStats;
    }

    public void setTopicsShardStats(ShardStats topicsShardStats) {
        this.topicsShardStats = topicsShardStats;
    }

    private SelectionTopDocuments filter(Query query, Set<String> selectedSources, SelectionTopDocuments selectResults) throws IOException {
        List<WikipediaDocument> topDocs = (List<WikipediaDocument>) selectResults.scoreDocs;
        for (WikipediaDocument topDoc : topDocs) {
            topDoc.setShardIds(topDoc.getCategories());
        }
        List<WikipediaDocument> shardedDocs = new ArrayList<>();
        if (selectedSources != null && !selectedSources.isEmpty()) {
            for (WikipediaDocument doc : topDocs) {
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

        int c_r = selectResults.totalHits;
        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(shardedDocs.size(), shardedDocs);
        selectionTopDocuments.setC_r(c_r);
        return selectionTopDocuments;
    }

    private SelectionTopDocuments filterTopics(Query query, Set<String> selectedTopics, SelectionTopDocuments selectResults) throws IOException {
        List<WikipediaDocument> topDocs = (List<WikipediaDocument>) selectResults.scoreDocs;
        List<WikipediaDocument> shardedDocs = new ArrayList<>();
        for (WikipediaDocument topDoc : topDocs) {
            topDoc.setShardIds(topDoc.getTopics());
        }
        if (selectedTopics != null && !selectedTopics.isEmpty()) {
            for (WikipediaDocument doc : topDocs) {
                ImmutableSortedSet<String> shardIds = new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                        .addAll(ImmutableSet.copyOf(doc.getShardIds()))
                        .build();
                if (Sets.intersection(selectedTopics, shardIds).size() > 0) {
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

    public SelectionTopDocuments isearch(boolean topics, Set<String> collections, String query, Filter filter, int n, boolean full) throws IOException, ParseException {
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

        SelectionTopDocuments selectionTopDocuments = new SelectionTopDocuments(totalHits, docs);

        if (topics) {
            return limit(filterTopics(q, collections, selectionTopDocuments), n);
        } else {
            return limit(filter(q, collections, selectionTopDocuments), n);
        }
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

    public SelectionTopDocuments search(boolean topics, Set<String> selected, String query, int limit, boolean full) throws IOException, ParseException {
        return isearch(topics, selected, query, null, limit, full);
    }

}
