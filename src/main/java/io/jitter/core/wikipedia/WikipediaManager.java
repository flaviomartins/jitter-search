package io.jitter.core.wikipedia;

import cc.twittertools.util.QueryLikelihoodModel;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.api.wikipedia.WikipediaDocument;
import io.jitter.core.utils.Stopper;
import io.jitter.core.utils.WikipediaSearchUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WikipediaManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(WikipediaManager.class);

    public static final String ID_FIELD = "id";
    public static final String TITLE_FIELD = "titleTokenized";
    public static final String TEXT_FIELD = "body";
    public static final String DATE_FIELD = "date";
    public static final String CATEGORIES_FIELD = "categories";
    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    public static final Analyzer ANALYZER = new EnglishAnalyzer();

    private final LMDirichletSimilarity similarity;
    private final QueryLikelihoodModel qlModel;

    private final String indexPath;
    private final boolean live;
    private Stopper stopper;
    private final float mu;
    private final PetscanCsvCategoryMapper categoryMapper;

    private DirectoryReader indexReader;
    private IndexSearcher searcher;
    private TaxonomyReader taxoReader;
    private FacetsConfig facetsConfig = new FacetsConfig();

    public WikipediaManager(String indexPath, boolean live, float mu, String cat2Topic) throws IOException {
        this.indexPath = indexPath;
        this.live = live;
        this.mu = mu;
        categoryMapper = new PetscanCsvCategoryMapper(new File(cat2Topic));

        similarity = new LMDirichletSimilarity(mu);
        qlModel = new QueryLikelihoodModel(mu);
    }

    public WikipediaManager(String indexPath, boolean live, String stopwords, float mu, String cat2Topic) throws IOException {
        this(indexPath, live, mu, cat2Topic);
        stopper = new Stopper(stopwords);
    }

    @Override
    public void start() throws Exception {
        try {
            searcher = getIndexSearcher();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        if (indexReader != null) {
            indexReader.close();
        }
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

    public WikipediaTopDocuments isearch(String query, Filter filter, int n, boolean full) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        float maxScore;
        int[] ids;
        float[] scores;

        IndexSearcher indexSearcher = getIndexSearcher();
        CollectionStats collectionStats = getCollectionStats();
        Query q = new QueryParser(TEXT_FIELD, ANALYZER).parse(query);

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
            HashSet<Object> topics = new HashSet<>();
            for (String cat : doc.getCategories()) {
                Set<String> catTopics = categoryMapper.getMap().get(cat);
                if (catTopics != null ) {
                    topics.addAll(catTopics);
                }
            }
            doc.setTopics(topics.toArray(new String[topics.size()]));

            if (!full) {
                doc.setText(StringUtils.abbreviate(doc.getText(), 500));
            }
        }

        return new WikipediaTopDocuments(totalHits, docs);
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

    public WikipediaTopDocuments search(String query, int limit, boolean full) throws IOException, ParseException {
        return isearch(query, null, limit, full);
    }
}