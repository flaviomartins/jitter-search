package io.jitter.core.taily;

import io.dropwizard.lifecycle.Managed;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.selection.Selection;
import io.jitter.core.selection.SelectionTopDocuments;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TailyManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(TailyManager.class);

    private final Analyzer analyzer;

    private final String dbPath;
    private final String index;
    private final float mu;
    private final float nc;
    private final List<String> users;
    private Map<String, List<String>> topics;

    private ShardRanker ranker;
    private ShardRanker topicsRanker;
    private boolean indexing;

    public TailyManager(String dbPath, String index, String stopwords, float mu, float nc, List<String> users) {
        this.dbPath = dbPath;
        this.index = index;
        this.mu = mu;
        this.nc = nc;
        this.users = users;

        Stopper stopper = null;
        if (!stopwords.isEmpty()) {
            stopper = new Stopper(stopwords);
        }
        if (stopper == null || stopper.asSet().isEmpty()) {
            analyzer = new TweetAnalyzer(CharArraySet.EMPTY_SET);
        } else {
            CharArraySet charArraySet = new CharArraySet(stopper.asSet(), true);
            analyzer = new TweetAnalyzer(charArraySet);
        }
    }

    public TailyManager(String dbPath, String index, String stopwords, float mu, float nc, List<String> users, Map<String, List<String>> topics) {
        this(dbPath, index, stopwords, mu, nc, users);
        this.topics = topics;
    }

    public List<String> getUsers() {
        return users;
    }

    public Map<String, List<String>> getTopics() {
        return topics;
    }

    private Map<String,Double> limit(Map<String, Double> ranking, int v) {
        Map<String, Double> map = new LinkedHashMap<>();
        ranking.entrySet().stream().filter(entry -> entry.getValue() >= v).forEachOrdered(entry -> map.put(entry.getKey(), entry.getValue()));
        return map;
    }

    public Map<String, Double> select(String query, int v) {
        return limit(ranker.rank(query), v);
    }

    public Map<String,Double> selectTopics(String query, int v) {
        return limit(topicsRanker.rank(query), v);
    }
    
    public int getDF(String source, String stem) {
        return ranker.getDF(source, stem);
    }

    public int getTopicsDF(String topic, String stem) {
        return topicsRanker.getDF(topic, stem);
    }

    @Override
    public void start() throws Exception {
        try {
            ranker = new ShardRanker(users, index, analyzer, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.SOURCES_DBENV);
            topicsRanker = new ShardRanker(topics.keySet(), index, analyzer, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.TOPICS_DBENV);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        if (ranker != null) {
            ranker.close();
            topicsRanker.close();
        }
    }

    public void index() throws Exception {
        if (indexing)
            return;

        try {
            indexing = true;
            if (ranker != null)
                ranker.close();
            if (topicsRanker != null)
                topicsRanker.close();

            Taily taily = new Taily(dbPath, index, mu);
            taily.build(users, topics);
        } catch (IOException e) {
            throw e;
        } finally {
            ranker = new ShardRanker(users, index, analyzer, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.SOURCES_DBENV);
            topicsRanker = new ShardRanker(topics.keySet(), index, analyzer, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.TOPICS_DBENV);
            indexing = false;
        }
    }

    public Map<String, Double> select(String query, int v, boolean topics) {
        Map<String, Double> ranking;
        if (topics) {
            ranking = selectTopics(query, v);
        } else {
            ranking = select(query, v);
        }
        return ranking;
    }

    public TailySelection selection(String query, int v, boolean topics) {
        return new TailySelection(query, v, topics).invoke();
    }

    public boolean isIndexing() {
        return indexing;
    }

    public class TailySelection implements Selection {
        private final String query;
        private final int v;
        private final boolean topics;
        private Map<String, Double> collections;

        public TailySelection(String query, int v, boolean topics) {
            this.query = query;
            this.v = v;
            this.topics = topics;
        }

        @Override
        public SelectionTopDocuments getResults() {
            return null;
        }

        @Override
        public Map<String, Double> getCollections() {
            return collections;
        }

        public TailySelection invoke() {
            if (topics) {
                collections = selectTopics(query, v);
            } else {
                collections = select(query, v);
            }
            return this;
        }
    }
}
