package org.novasearch.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.novasearch.jitter.core.selection.SelectionManager;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SelectionManagerHealthCheck extends HealthCheck {
    private static final Logger logger = Logger.getLogger(SelectionManagerHealthCheck.class);

    private final SelectionManager selectionManager;

    public SelectionManagerHealthCheck(SelectionManager selectionManager) {
        this.selectionManager = selectionManager;
    }

    @Override
    protected Result check() throws Exception {
        String index = selectionManager.getIndex();
        if (index == null) {
            return Result.unhealthy("SelectionManager doesn't have any index");
        }

        Map<String, List<String>> topics = selectionManager.getTopics();
        Set<String> topicAccounts = new TreeSet<>();
        for (String topic : topics.keySet()) {
            topicAccounts.addAll(topics.get(topic));
        }

        Set<String> twitterAccounts = new TreeSet<>();
        String twitterMode = selectionManager.getTwitterMode();
        if ("archiver".equals(twitterMode)) {
            twitterAccounts.addAll(selectionManager.getTwitterArchiver().getUsers());
        } else if ("standard".equals(twitterMode)) {
            twitterAccounts.addAll(selectionManager.getTwitterManager().getUsers());
        } else {
            return Result.unhealthy("Invalid Twitter mode");
        }

        Sets.SetView<String> diff1 = Sets.difference(twitterAccounts, topicAccounts);
        if (diff1.size() > 0) {
            logger.warn("missing from topics: " + Joiner.on(" ").join(diff1));
        }

        Sets.SetView<String> diff2 = Sets.difference(topicAccounts, twitterAccounts );
        if (diff2.size() > 0) {
            logger.error("missing from twitter: " + Joiner.on(" ").join(diff2));
            return Result.unhealthy("missing from twitter: " + Joiner.on(" ").join(diff2));
        }

        return Result.healthy();
    }
}
