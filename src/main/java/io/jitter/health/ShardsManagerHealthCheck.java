package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import io.jitter.core.shards.ShardsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ShardsManagerHealthCheck extends HealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManagerHealthCheck.class);

    private final ShardsManager shardsManager;

    public ShardsManagerHealthCheck(ShardsManager shardsManager) {
        this.shardsManager = shardsManager;
    }

    @Override
    protected Result check() throws Exception {
        String index = shardsManager.getIndexPath();
        if (index == null) {
            return Result.unhealthy("SelectionManager doesn't have any index");
        }

        Map<String, ImmutableSortedSet<String>> topics = shardsManager.getTopics();
        Set<String> topicAccounts = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, ImmutableSortedSet<String>> entry : topics.entrySet()) {
            topicAccounts.addAll(entry.getValue());
        }

        ImmutableSortedSet<String> users = shardsManager.getTwitterManager().getUsers();
        Set<String> twitterAccounts = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        twitterAccounts.addAll(users);

        Sets.SetView<String> diff1 = Sets.difference(twitterAccounts, topicAccounts);
        if (diff1.size() > 0) {
            logger.warn("missing from topics: {}", Joiner.on(" ").join(diff1));
        }

        Sets.SetView<String> diff2 = Sets.difference(topicAccounts, twitterAccounts );
        if (diff2.size() > 0) {
            String missing = Joiner.on(" ").join(diff2);
            logger.error("missing from twitter: {}", missing);
            return Result.unhealthy("missing from twitter: " + missing);
        }

        return Result.healthy();
    }
}