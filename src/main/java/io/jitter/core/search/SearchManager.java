package io.jitter.core.search;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.utils.SearchUtils;
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
import io.jitter.api.search.StatusDocument;
import io.jitter.core.utils.Stopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class SearchManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SearchManager.class);
    
    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private final Analyzer analyzer;
    private final LMDirichletSimilarity similarity;
    private final QueryLikelihoodModel qlModel;

    private final String indexPath;
    private Stopper stopper;
    private final float mu;
    private final boolean live;

    private DirectoryReader indexReader;
    private IndexSearcher searcher;

    public SearchManager(String indexPath, String stopwords, float mu, boolean live) {
        this.indexPath = indexPath;
        this.mu = mu;
        this.live = live;

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

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public Stopper getStopper() {
        return stopper;
    }

    public float getMu() {
        return mu;
    }

    public TopDocuments isearch(String query, String filterQuery, Filter filter, int n, boolean filterRT) throws IOException, ParseException {
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
        List<StatusDocument> docs = SearchUtils.getDocs(indexSearcher, analyzer, collectionStats, qlModel, topDocs, query, n, filterRT, true);

        return new TopDocuments(totalHits, docs);
    }

    public TopDocuments isearch(String query, String filterQuery, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, filterQuery, null, n, filterRT);
    }

    public TopDocuments isearch(String query, String filterQuery, int n) throws IOException, ParseException {
        return isearch(query, filterQuery, null, n, false);
    }

    public TopDocuments search(String query, String filterQuery, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        return isearch(query, filterQuery, filter, n, filterRT);
    }

    public TopDocuments search(String query, String filterQuery, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        return isearch(query, filterQuery, filter, n, filterRT);
    }

    public TopDocuments search(String query, String filterQuery, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, filterQuery, n, filterRT);
    }

    public TopDocuments search(String query, String filterQuery, int n) throws IOException, ParseException {
        return isearch(query, filterQuery, n);
    }

    public void forceMerge() throws IOException {
        logger.info("Merging started!");
        long startTime = System.currentTimeMillis();
        Path indexPath = Paths.get(this.indexPath);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
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

    public TopDocuments search(String query, String filterQuery, Optional<Long> maxId, int limit, boolean retweets, long[] epochs, boolean future) throws IOException, ParseException {
        TopDocuments results;
        if (!future) {
            if (maxId.isPresent()) {
                results = search(query, filterQuery, limit, !retweets, maxId.get());
            } else if (epochs[0] > 0 || epochs[1] > 0) {
                results = search(query, filterQuery, limit, !retweets, epochs[0], epochs[1]);
            } else {
                results = search(query, filterQuery, limit, !retweets);
            }
        } else {
            results = search(query, filterQuery, limit, !retweets, Long.MAX_VALUE);
        }
        return results;
    }

    public TopDocuments search(String query, String filterQuery, Optional<Long> maxId, int limit, boolean retweets, long[] epochs) throws IOException, ParseException {
        return search(query, filterQuery, maxId, limit, retweets, epochs, false);
    }
}
