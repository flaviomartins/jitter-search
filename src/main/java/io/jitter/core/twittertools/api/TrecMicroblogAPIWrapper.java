package io.jitter.core.twittertools.api;

import cc.twittertools.search.api.TrecSearchThriftClient;
import cc.twittertools.thrift.gen.TResult;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.search.TopDocuments;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Stopper;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

public class TrecMicroblogAPIWrapper implements Managed {
    private static final Logger LOG = Logger.getLogger(TrecMicroblogAPIWrapper.class);

    public static final float DEFAULT_MU = 2500.0f;

    private static final int MAX_NUM_RESULTS = 10000;
    private static final int DEFAULT_NUM_RESULTS = 3000;

    private static final Analyzer ANALYZER = new TweetAnalyzer();

    private final String host;
    private final int port;
    private final TrecSearchThriftClient client;
    private final String cacheDir;
    private final boolean useCache;
    private Stopper stopper;
    private CollectionStats collectionStats;

    public TrecMicroblogAPIWrapper(String host, int port, @Nullable String group,
                                   @Nullable String token, @Nullable String cacheDir, boolean useCache) {
        Preconditions.checkNotNull(host);
        Preconditions.checkArgument(port > 0);
        this.host = host;
        this.port = port;
        this.client = new TrecSearchThriftClient(host, port, group, token);
        this.cacheDir = cacheDir;
        this.useCache = useCache;
    }

    public TrecMicroblogAPIWrapper(String host, int port, String group, String token, String cacheDir, boolean useCache, String stopwords, @Nullable String stats, @Nullable String statsDb) {
        this(host, port, group, token, cacheDir, useCache);
        stopper = new Stopper(stopwords);
        if (stats != null && statsDb != null) {
            collectionStats = new TrecCollectionStats(stats, statsDb);
        }
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

    public TopDocuments search(String query, long maxId, int numResults) throws TException,
            IOException, ClassNotFoundException, ParseException {
        return search(query, maxId, numResults, false);
    }

    @SuppressWarnings("unchecked")
    public TopDocuments search(String query, long maxId, int numResults, boolean filterRT) throws TException,
            IOException, ClassNotFoundException, ParseException {

        int numResultsToFetch = Math.min(MAX_NUM_RESULTS, Math.max(DEFAULT_NUM_RESULTS, numResults));

        String cacheFileName = DigestUtils.sha1Hex(host + port + query + maxId + numResultsToFetch);
        File f = new File(cacheDir + cacheFileName);
        List<TResult> results;

        if (useCache) {
            if (f.exists()) {
                LOG.info("Reading " + f.getPath() + ".");
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(f)));
                results = (List<TResult>) ois.readObject();
                ois.close();
                LOG.info("Read " + results.size() + " results.");
            } else {
                LOG.warn("Cache file not found: " + f.getPath()
                        + ". Connecting to the server...");
                results = client.search(query, maxId, numResultsToFetch, true);
                synchronized (this) {
                    ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
                    oos.writeObject(results);
                    oos.close();
                }
                LOG.warn("Writing " + results.size() + " results: " + f.getPath() + ".");
            }
        } else {
            results = client.search(query, maxId, numResultsToFetch, true);
        }

        // Convert to custom class
        Iterator<TResult> resultIt = results.iterator();

        LongOpenHashSet seenSet = new LongOpenHashSet();
        List<StatusDocument> updatedResults = new ArrayList<>(results.size());
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

            StatusDocument updatedResult = new StatusDocument(origResult);
            updatedResults.add(updatedResult);
        }

        int totalDF = 0;
        if (collectionStats != null) {
            Set<String> qTerms = new LinkedHashSet<>();
            for (String term : AnalyzerUtils.analyze(ANALYZER, query)) {
                if (!term.isEmpty()) {
                    qTerms.add(term);
                }
            }
            for (String term : qTerms) {
                totalDF += collectionStats.docFreq(term);
            }
        }

        List<StatusDocument> documents = updatedResults.subList(0, Math.min(updatedResults.size(), numResults));
        
        int totalHits = totalDF > 0 ? totalDF : documents.size();
        return new TopDocuments(totalHits, documents);
    }

    public TopDocuments search(String query, Optional<Long> maxId, int limit, boolean retweets, boolean future) throws ClassNotFoundException, TException, ParseException, IOException {
        TopDocuments selectResults;
        if (!future) {
            if (maxId.isPresent()) {
                selectResults = search(query, maxId.get(), limit, !retweets);
            } else {
                selectResults = search(query, Long.MAX_VALUE, limit, !retweets);
            }
        } else {
            selectResults = search(query, Long.MAX_VALUE, limit, !retweets);
        }
        return selectResults;
    }

    public TopDocuments search(String query, Optional<Long> maxId, int limit, boolean retweets) throws TException, IOException, ClassNotFoundException, ParseException {
        return search(query, maxId, limit, retweets, false);
    }
}
