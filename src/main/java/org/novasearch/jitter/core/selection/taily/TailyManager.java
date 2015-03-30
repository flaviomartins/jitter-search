package org.novasearch.jitter.core.selection.taily;

import io.dropwizard.lifecycle.Managed;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TailyManager implements Managed {
    private static final Logger logger = Logger.getLogger(TailyManager.class);

    private String dbPath;
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

    public Map<String, Double> getRanked(String query) {
        return ranker.rank(query);
    }

    public Map<String,Double> getRankedTopics(String query) {
        return topicsRanker.rank(query);
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

    public void index() throws IOException {
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
