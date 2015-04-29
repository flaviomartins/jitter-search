package org.novasearch.jitter.core.search;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.thrift.gen.TResult;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.search.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class SearchManager implements Managed {

    private static final Logger logger = LoggerFactory.getLogger(SearchManager.class);

    public static final int MAX_RESULTS = 10000;
    public static final int MAX_TERMS_RESULTS = 1000;

    private static final QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);

    private final String indexPath;
    private final String databasePath;

    private DirectoryReader reader;
    private IndexSearcher searcher;

    public SearchManager(String indexPath, String databasePath) {
        this.indexPath = indexPath;
        this.databasePath = databasePath;
    }

    @Override
    public void start() throws Exception {
        try {
            reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
            searcher = new IndexSearcher(reader);
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
        int numResults = n > MAX_RESULTS ? MAX_RESULTS : n;
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

            p.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
            p.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();

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

    public List<Document> getSorted(TopDocs rs, boolean filterRT) throws IOException {
        List<Document> results = getResults(rs);
        return sortResults(results, filterRT);
    }

    private List<Document> sortResults(List<Document> results, boolean filterRT) {
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
        int dupliCount = 0;
        double rsvPrev = 0;
        for (DocumentComparable sortedResult : sortedResults) {
            Document result = sortedResult.getDocument();
            double rsvCurr = result.rsv;
            if (Math.abs(rsvCurr - rsvPrev) > 0.0000001) {
                dupliCount = 0;
            } else {
                dupliCount++;
                rsvCurr = rsvCurr - 0.000001 / results.size() * dupliCount;
            }
            // FIXME: what is this?
            result.rsv = rsvCurr;

            docs.add(new Document(result));
            i++;
            rsvPrev = result.rsv;
        }

        return docs;
    }

    private void createDatabase() {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            statement.executeUpdate("create table tweets (" +
                    "id integer, " +
                    "screen_name string, " +
                    "epoch integer, " +
                    "text string, " +
                    "followers_count integer, " +
                    "statuses_count integer, " +
                    "lang string, " +
                    "in_reply_to_status_id integer, " +
                    "in_reply_to_user_id integer, " +
                    "retweeted_status_id integer, " +
                    "retweeted_user_id integer, " +
                    "retweet_count integer, " +
                    "PRIMARY KEY (id))");
        } catch (SQLException e) {
            // if the error message is "out of memory",
            // it probably means no database file is found
            logger.error(e.getMessage());
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e.getMessage());
            }
        }
    }

    private void addDocumentsToDatabase(List<TResult> results) {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
            connection.setAutoCommit(false);

            for (TResult result : results) {
                try {
                    PreparedStatement statement = connection.prepareStatement(
                            "INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
                    statement.setLong(1, result.id);
                    statement.setString(2, result.screen_name);
                    statement.setLong(3, result.epoch);
                    statement.setString(4, result.text);
                    statement.setLong(5, result.followers_count);
                    statement.setLong(6, result.statuses_count);
                    statement.setString(7, result.lang);
                    statement.setLong(8, result.in_reply_to_status_id);
                    statement.setLong(9, result.in_reply_to_user_id);
                    statement.setLong(10, result.retweeted_status_id);
                    statement.setLong(11, result.retweeted_user_id);
                    statement.setLong(12, result.retweeted_count);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    if (!e.getMessage().startsWith("[SQLITE_CONSTRAINT]"))
                        logger.error(e.getMessage());
                }
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            // if the error message is "out of memory",
            // it probably means no database file is found
            logger.error(e.getMessage());
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                logger.error(e.getMessage());
            }
        }

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

                    writer.addDocument(doc);
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
            e.printStackTrace();
        } finally {
            dir.close();
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
        return HighFreqTerms.getHighFreqTerms(reader, numResults, IndexStatuses.StatusField.TEXT.name);
    }

    public IndexSearcher getSearcher() throws IOException {
        try {
            if (reader == null) {
                reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
                searcher = new IndexSearcher(reader);
                searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
            } else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                    searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
                }
            }
        } catch (IndexNotFoundException e) {
            logger.error(e.getMessage());
        }
        return searcher;
    }
}
