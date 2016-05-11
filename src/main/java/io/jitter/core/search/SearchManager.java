package io.jitter.core.search;

import cc.twittertools.index.IndexStatuses;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.collectionstatistics.IndexCollectionStats;
import io.jitter.core.utils.SearchUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import io.jitter.api.search.Document;
import io.jitter.core.utils.Stopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class SearchManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SearchManager.class);
    
    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private static final Analyzer analyzer = IndexStatuses.ANALYZER;
    private static final QueryParser QUERY_PARSER =
            new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer);
    public static final Similarity SIMILARITY = new LMDirichletSimilarity(2500);

    private final String indexPath;
    private final String databasePath;
    private final boolean live;
    private Stopper stopper;

    private DirectoryReader reader;
    private IndexSearcher searcher;

    public SearchManager(String indexPath, String databasePath, boolean live) {
        this.indexPath = indexPath;
        this.databasePath = databasePath;
        this.live = live;
    }

    public SearchManager(String indexPath, String databasePath, boolean live, String stopwords) {
        this(indexPath, databasePath, live);
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

    public Stopper getStopper() {
        return stopper;
    }

    public void setStopper(Stopper stopper) {
        this.stopper = stopper;
    }

    public TopDocuments isearch(String query, Filter filter, int n, boolean filterRT) throws IOException, ParseException {
        int len = Math.min(MAX_RESULTS, 3 * n);
        int nDocsReturned;
        int totalHits;
        float maxScore;
        int[] ids;
        float[] scores;

        IndexSearcher indexSearcher = getIndexSearcher();
        Query q = QUERY_PARSER.parse(query.replaceAll(",", ""));

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

        List<Document> docs = SearchUtils.getDocs(indexSearcher, topDocs, n, filterRT, true);
        if (filterRT) {
            logger.info("filter_rt count: {}", nDocsReturned - docs.size());
        }

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

    public void index() throws IOException {
        logger.info("Indexing started!");
        File indexPath = new File(this.indexPath);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);
        textOptions.setStoreTermVectors(true);

        Connection connection;
        int cnt = 0;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
            connection.setAutoCommit(false);

            try {
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT * FROM tweets;");
                ResultSet rs = statement.executeQuery();

                while (rs.next()) {
                    cnt++;
                    org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                    long id = rs.getLong(IndexStatuses.StatusField.ID.name);
                    doc.add(new LongField(IndexStatuses.StatusField.ID.name, id, Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, rs.getLong(IndexStatuses.StatusField.EPOCH.name), Field.Store.YES));
                    doc.add(new TextField(IndexStatuses.StatusField.SCREEN_NAME.name, rs.getString(IndexStatuses.StatusField.SCREEN_NAME.name), Field.Store.YES));

                    doc.add(new Field(IndexStatuses.StatusField.TEXT.name, rs.getString(IndexStatuses.StatusField.TEXT.name), textOptions));

//                    doc.add(new IntField(IndexStatuses.StatusField.FRIENDS_COUNT.name, rs.getInt(IndexStatuses.StatusField.FRIENDS_COUNT.name), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name, rs.getInt(IndexStatuses.StatusField.FOLLOWERS_COUNT.name), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.STATUSES_COUNT.name, rs.getInt(IndexStatuses.StatusField.STATUSES_COUNT.name), Field.Store.YES));

                    long inReplyToStatusId = rs.getLong(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name);
                    if (inReplyToStatusId > 0) {
                        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
                        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name, rs.getLong(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name), Field.Store.YES));
                    }

                    String lang = rs.getString(IndexStatuses.StatusField.LANG.name);
                    if (lang != null && !"unknown".equals(lang)) {
                        doc.add(new TextField(IndexStatuses.StatusField.LANG.name, lang, Field.Store.YES));
                    }

                    long retweetStatusId = rs.getLong(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name);
                    if (retweetStatusId > 0) {
                        doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
                        doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_USER_ID.name, rs.getLong(IndexStatuses.StatusField.RETWEETED_USER_ID.name), Field.Store.YES));
                        int retweetCount = rs.getInt(IndexStatuses.StatusField.RETWEET_COUNT.name);
                        doc.add(new IntField(IndexStatuses.StatusField.RETWEET_COUNT.name, retweetCount, Field.Store.YES));
                        if (retweetCount < 0 || retweetStatusId < 0) {
                            logger.warn("Error parsing retweet fields of {}", id);
                        }
                    }

                    Term delTerm = new Term(IndexStatuses.StatusField.ID.name, Long.toString(id));
                    writer.updateDocument(delTerm, doc);
                    if (cnt % 10000 == 0) {
                        logger.info("{} statuses indexed", cnt);
                    }
                }

            } catch (SQLException e) {
                // if the error message is "out of memory",
                // it probably means no database file is found
                logger.error(e.getMessage());
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // connection close failed.
                    logger.error(e.getMessage());
                }
            }
            logger.info("Total of {} statuses added", cnt);
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        } finally {
            dir.close();
        }
    }

    public void forceMerge() throws IOException {
        logger.info("Merging started!");
        long startTime = System.currentTimeMillis();
        File indexPath = new File(this.indexPath);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
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

    public String getIndexPath() {
        return indexPath;
    }

    public String getDatabasePath() {
        return databasePath;
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

    public CollectionStats getCollectionStats() {
        return new IndexCollectionStats(reader, IndexStatuses.StatusField.TEXT.name);
    }

    public TopDocuments search(int limit, boolean retweets, boolean future, Optional<Long> maxId, Optional<String> epoch, String query, long[] epochs) throws IOException, ParseException {
        TopDocuments results;
        if (!future) {
            if (maxId.isPresent()) {
                results = search(query, limit, !retweets, maxId.get());
            } else if (epoch.isPresent()) {
                results = search(query, limit, !retweets, epochs[0], epochs[1]);
            } else {
                results = search(query, limit, !retweets);
            }
        } else {
            results = search(query, limit, !retweets, Long.MAX_VALUE);
        }
        return results;
    }

    public TopDocuments search(int limit, boolean retweets, Optional<Long> maxId, Optional<String> epoch, String query, long[] epochs) throws IOException, ParseException {
        return search(limit, retweets, false, maxId, epoch, query, epochs);
    }
}
