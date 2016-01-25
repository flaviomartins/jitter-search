package io.jitter.core.taily;

import io.dropwizard.lifecycle.Managed;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TailyManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(TailyManager.class);

    private final String dbPath;
    private final String index;
    private final int mu;
    private final int nc;
    private final List<String> users;
    private Map<String, List<String>> topics;

    private ShardRanker ranker;
    private ShardRanker topicsRanker;

    public TailyManager(String dbPath, String index, int mu, int nc, List<String> users) {
        this.dbPath = dbPath;
        this.index = index;
        this.mu = mu;
        this.nc = nc;
        this.users = users;
    }

    public TailyManager(String dbPath, String index, int mu, int nc, List<String> users, Map<String, List<String>> topics) {
        this(dbPath, index, mu, nc, users);
        this.topics = topics;
    }

    private Map<String,Double> limit(Map<String, Double> ranking, int v) {
        Map<String, Double> map = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : ranking.entrySet()) {
            if (entry.getValue() >= v) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
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
            ranker = new ShardRanker(users, index, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.SOURCES_DBENV);
            topicsRanker = new ShardRanker(topics.keySet().toArray(new String[topics.keySet().size()]), index, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.TOPICS_DBENV);
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

    public void index() throws IOException, ParseException {
        if (ranker != null) {
            ranker.close();
            topicsRanker.close();
        }

        Taily taily = new Taily(dbPath, index, mu);
        taily.buildCorpus();
        taily.buildFromSources(users);
        taily.buildFromTopics(topics);

        ranker = new ShardRanker(users, index, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.SOURCES_DBENV);
        topicsRanker = new ShardRanker(topics.keySet().toArray(new String[topics.keySet().size()]), index, nc, dbPath + "/" + Taily.CORPUS_DBENV, dbPath + "/" + Taily.TOPICS_DBENV);
    }

}
