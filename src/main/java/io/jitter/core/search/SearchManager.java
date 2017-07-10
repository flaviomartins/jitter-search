package io.jitter.core.search;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.utils.SearchUtils;
import org.apache.lucene.analysis.Analyzer;
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

    private static final Analyzer ANALYZER = new TweetAnalyzer();

    private final LMDirichletSimilarity similarity;
    private final QueryLikelihoodModel qlModel;

    private final String indexPath;
    private final boolean live;
    private Stopper stopper;
    private final float mu;

    private DirectoryReader reader;
    private IndexSearcher searcher;

    public SearchManager(String indexPath, boolean live, float mu) {
        this.indexPath = indexPath;
        this.live = live;
        this.mu = mu;

        similarity = new LMDirichletSimilarity(mu);
        qlModel = new QueryLikelihoodModel(mu);
    }

    public SearchManager(String indexPath, boolean live, String stopwords, float mu) {
        this(indexPath, live, mu);
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
        if (reader != null) {
            reader.close();
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

    public TopDocuments isearch(String query, Filter filter, int n, boolean filterRT) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        float maxScore;
        int[] ids;
        float[] scores;

        IndexSearcher indexSearcher = getIndexSearcher();
        CollectionStats collectionStats = getCollectionStats();
        Query q = new QueryParser(IndexStatuses.StatusField.TEXT.name, ANALYZER).parse(query);

        final TopDocsCollector topCollector = TopScoreDocCollector.create(len, null);
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

        // Compute real QL scores even when live to build termVectors
        List<StatusDocument> docs = SearchUtils.getDocs(indexSearcher, collectionStats, qlModel, topDocs, query, n, filterRT, true);

        return new TopDocuments(totalHits, docs);
    }

    public TopDocuments isearch(String query, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, null, n, filterRT);
    }

    public TopDocuments isearch(String query, int n) throws IOException, ParseException {
        return isearch(query, null, n, false);
    }

    public TopDocuments search(String query, int n, boolean filterRT, long maxId) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.ID.name, 0L, maxId, true, true);
        return isearch(query, filter, n, filterRT);
    }

    public TopDocuments search(String query, int n, boolean filterRT, long firstEpoch, long lastEpoch) throws IOException, ParseException {
        Filter filter =
                NumericRangeFilter.newLongRange(IndexStatuses.StatusField.EPOCH.name, firstEpoch, lastEpoch, true, true);
        return isearch(query, filter, n, filterRT);
    }

    public TopDocuments search(String query, int n, boolean filterRT) throws IOException, ParseException {
        return isearch(query, n, filterRT);
    }

    public TopDocuments search(String query, int n) throws IOException, ParseException {
        return isearch(query, n);
    }

    public void forceMerge() throws IOException {
        logger.info("Merging started!");
        long startTime = System.currentTimeMillis();
        Path indexPath = Paths.get(this.indexPath);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
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
                reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
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

    public TopDocuments search(String query, Optional<Long> maxId, int limit, boolean retweets, long[] epochs, boolean future) throws IOException, ParseException {
        TopDocuments results;
        if (!future) {
            if (maxId.isPresent()) {
                results = search(query, limit, !retweets, maxId.get());
            } else if (epochs[0] > 0 || epochs[1] > 0) {
                results = search(query, limit, !retweets, epochs[0], epochs[1]);
            } else {
                results = search(query, limit, !retweets);
            }
        } else {
            results = search(query, limit, !retweets, Long.MAX_VALUE);
        }
        return results;
    }

    public TopDocuments search(String query, Optional<Long> maxId, int limit, boolean retweets, long[] epochs) throws IOException, ParseException {
        return search(query, maxId, limit, retweets, epochs, false);
    }
}
