package org.novasearch.jitter.core.selection.taily;

import io.dropwizard.lifecycle.Managed;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TailyManager implements Managed {
    private static final Logger logger = Logger.getLogger(TailyManager.class);

    private final String index;
    private final int nc;
    private final List<String> users;

    private ShardRanker ranker;

    public TailyManager(String index, int nc, List<String> users) {
        this.index = index;
        this.nc = nc;
        this.users = users;
    }

    public Map<String, Double> getRanked(String query) {
        return ranker.rank(query);
    }

    @Override
    public void start() throws Exception {
        try {
            ranker = new ShardRanker(users, index, nc);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        if (ranker != null)
            ranker.close();
    }

    public void index() throws IOException {
        if (ranker != null)
            ranker.close();

        Taily taily = new Taily(index);
        taily.buildCorpus();
        taily.buildFromMap(users);

        ranker = new ShardRanker(users, index, nc);
    }
}
