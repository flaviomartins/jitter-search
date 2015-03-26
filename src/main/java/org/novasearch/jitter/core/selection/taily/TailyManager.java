package org.novasearch.jitter.core.selection.taily;

import io.dropwizard.lifecycle.Managed;

import java.util.List;
import java.util.Map;

public class TailyManager implements Managed {

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
        ranker = new ShardRanker(users, index, nc);
    }

    @Override
    public void stop() throws Exception {
        ranker.close();
    }
}
