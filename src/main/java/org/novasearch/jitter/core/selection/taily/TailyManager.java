package org.novasearch.jitter.core.selection.taily;

import io.dropwizard.lifecycle.Managed;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TailyManager implements Managed {
    private static final Logger logger = Logger.getLogger(TailyManager.class);

    private final String index;
    private final int mu;
    private final int nc;
    private final List<String> users;
    private Map<String, List<String>> topics;

    private ShardRanker ranker;
    private ShardRanker topicsRanker;

    public TailyManager(String index, int mu, int nc, List<String> users) {
        this.index = index;
        this.mu = mu;
        this.nc = nc;
        this.users = users;
    }

    public TailyManager(String index, int mu, int nc, List<String> users, Map<String, List<String>> topics) {
        this(index, mu, nc, users);
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
            ranker = new ShardRanker(users, index, nc, "taily/bdbmap");
            topicsRanker = new ShardRanker(topics.keySet().toArray(new String[topics.keySet().size()]), index, nc, "taily/bdbmaptopics");
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

        Taily taily = new Taily(index, mu);
        taily.buildCorpus();
        taily.buildFromMap(users);
        taily.buildFromMapTopics(topics);

        ranker = new ShardRanker(users, index, nc, "taily/bdbmap");
        topicsRanker = new ShardRanker(topics.keySet().toArray(new String[topics.keySet().size()]), index, nc, "taily/bdbmaptopics");
    }

}
