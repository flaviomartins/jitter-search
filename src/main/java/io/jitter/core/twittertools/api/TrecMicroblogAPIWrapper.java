package io.jitter.core.twittertools.api;

import cc.twittertools.search.api.TrecSearchThriftClient;
import cc.twittertools.thrift.gen.TResult;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Stopper;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class TrecMicroblogAPIWrapper implements Managed {
    private static final Logger LOG = Logger.getLogger(TrecMicroblogAPIWrapper.class);

    public static final int MAX_NUM_RESULTS = 10000;

    private final String host;
    private final int port;
    private final TrecSearchThriftClient client;
    private final String cacheDir;
    private final boolean useCache;
    private final String collectDb;
    private Stopper stopper;
    private CollectionStats collectionStats;

    public TrecMicroblogAPIWrapper(String host, int port, @Nullable String group,
                                   @Nullable String token, @Nullable String cacheDir, boolean useCache, @Nullable String collectDb) {
        Preconditions.checkNotNull(host);
        Preconditions.checkArgument(port > 0);
        this.host = host;
        this.port = port;
        this.client = new TrecSearchThriftClient(host, port, group, token);
        this.cacheDir = cacheDir;
        this.useCache = useCache;
        this.collectDb = collectDb;
        if (collectDb != null)
            createDatabase();
    }

    public TrecMicroblogAPIWrapper(String host, int port, String group, String token, String cacheDir, boolean useCache, String collectDb, String stopwords, @Nullable String stats) {
        this(host, port, group, token, cacheDir, useCache, collectDb);
        stopper = new Stopper(stopwords);
        if (stats != null)
            collectionStats = new TrecCollectionStats(stats);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    public Stopper getStopper() {
        return stopper;
    }

    public void setStopper(Stopper stopper) {
        this.stopper = stopper;
    }

    public CollectionStats getCollectionStats() {
        return collectionStats;
    }

    public void setCollectionStats(TrecCollectionStats collectionStats) {
        this.collectionStats = collectionStats;
    }

    public TopDocuments search(String query, long maxId, int numResults)  throws TException,
            IOException, ClassNotFoundException {
        return search(query, maxId, numResults, false);
    }

    @SuppressWarnings("unchecked")
    public TopDocuments search(String query, long maxId, int numResults, boolean filterRT) throws TException,
            IOException, ClassNotFoundException {

        int numResultsToFetch = Math.min(MAX_NUM_RESULTS, 3 * numResults);

        String cacheFileName = DigestUtils.shaHex(host + port + query + maxId + numResultsToFetch);
        File f = new File(cacheDir + cacheFileName);
        List<TResult> results;
        boolean fromCache = false;

        if (useCache) {
            if (f.exists()) {
                fromCache = true;
                LOG.info("Reading " + f.getPath() + ".");
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
                results = (List<TResult>) ois.readObject();
                ois.close();
                LOG.info("Read " + results.size() + " results.");
            } else {
                LOG.warn("Cache file not found: " + f.getPath()
                        + ". Connecting to the server...");
                results = client.search(query, maxId, numResultsToFetch);
                ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
                oos.writeObject(results);
                oos.close();
                LOG.warn("Writing " + results.size() + " results: " + f.getPath() + ".");
            }
        } else {
            results = client.search(query, maxId, numResults);
        }

        // only need to add results to database when fetching from server for the first time
        if (collectDb != null && !fromCache)
            addResultsToDatabase(results);

        // Convert to custom class
        Iterator<TResult> resultIt = results.iterator();

        LongOpenHashSet seenSet = new LongOpenHashSet();
        List<Document> updatedResults = new ArrayList<>(results.size());
        while (resultIt.hasNext()) {
            TResult origResult = resultIt.next();

            if (filterRT) {
                if (origResult.getRetweeted_status_id() != 0)
                    continue;

                if (StringUtils.startsWithIgnoreCase(origResult.getText(), "RT "))
                    continue;
            }

            if (seenSet.contains(origResult.getId()))
                continue;

            seenSet.add(origResult.getId());

            Document updatedResult = new Document(origResult);
            updatedResults.add(updatedResult);
        }

        int totalDF = 0;
        if (collectionStats != null) {
            for (String term : AnalyzerUtils.analyze(new StopperTweetAnalyzer(Version.LUCENE_43, false, false), query)) {
                if (term.isEmpty())
                    continue;
                if ("AND".equals(term) || "OR".equals(term))
                    continue;
                totalDF += collectionStats.getDF(term);
            }
        }

        List<Document> documents = updatedResults.subList(0, Math.min(updatedResults.size(), numResults));
        
        int totalHits = totalDF > 0 ? totalDF : documents.size();
        return new TopDocuments(totalHits, documents);
    }

    private void createDatabase() {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + collectDb);
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
                    "retweeted_count integer, " +
                    "PRIMARY KEY (id))");
        } catch (SQLException e) {
            // if the error message is "out of memory",
            // it probably means no database file is found
            LOG.error(e.getMessage());
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                LOG.error(e);
            }
        }
    }

    private void addResultsToDatabase(List<TResult> results) {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + collectDb);
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
                        LOG.error(e.getMessage());
                }
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            // if the error message is "out of memory",
            // it probably means no database file is found
            LOG.error(e.getMessage());
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                LOG.error(e);
            }
        }

    }
}
