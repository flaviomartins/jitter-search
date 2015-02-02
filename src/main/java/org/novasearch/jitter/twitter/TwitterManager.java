package org.novasearch.jitter.twitter;

import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TwitterManager implements Managed {
    final static Logger logger = LoggerFactory.getLogger(TwitterManager.class);

    private static final int MAX_USERS_LOOKUP = 100;
    private static final int MAX_STATUSES_REQUEST = 200;

    private List<String> screenNames;
    private Map<String, User> usersMap;
    private Map<String, UserTimeline> userTimelines;

    // The factory instance is re-useable and thread safe.
    private Twitter twitter = TwitterFactory.getSingleton();

    public TwitterManager(List<String> screenNames) {
        this.screenNames = screenNames;
        this.usersMap = new LinkedHashMap<>();
        this.userTimelines = new LinkedHashMap<>();
    }

    @Override
    public void start() throws Exception {
//        lookupUsers();
    }

    public void lookupUsers() {
        int remaining = screenNames.size();
        logger.info(remaining + " total users");

        int i = 0;
        do {
            int reqSize = Math.min(remaining, MAX_USERS_LOOKUP);

            String[] names = screenNames.subList(i, i + reqSize).toArray(new String[0]);
            ResponseList<User> userResponseList;
            try {
                logger.info(reqSize + " users info requested");
                userResponseList = twitter.lookupUsers(names);
                for (User user : userResponseList) {
                    logger.info("Got info for " + user.getScreenName() + " : " + user.getName() + " : " + user.getStatusesCount());
                    usersMap.put(user.getScreenName(), user);
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
            remaining -= reqSize;
            i += reqSize;
        } while (remaining > 0);
    }

    @Override
    public void stop() throws Exception {

    }

    public UserTimeline getUserTimeline(String screenName) {
        return userTimelines.get(screenName);
    }

    public void archive() {
        lookupUsers();
        for (String screenName : screenNames) {
            User user = usersMap.get(screenName);
            if (user == null)
                logger.warn("Failed to lookup " + screenName);

            UserTimeline timeline;
            if (userTimelines.get(screenName) != null) {
                timeline = userTimelines.get(screenName);
            } else {
                timeline = new UserTimeline(user);
                userTimelines.put(screenName, timeline);
            }

            long sinceId = timeline.getLatestId();
            try {
                if (user.getStatus() != null) {
                    int page = 1;
                    logger.info(screenName + " since_id: " + sinceId);
                    Paging paging = new Paging(page, MAX_STATUSES_REQUEST).sinceId(sinceId);
                    for (;;page++) {
                        paging.setPage(page);
                        logger.info(screenName + " page: " + page);
                        List<Status> statuses = twitter.getUserTimeline(user.getId(), paging);
                        if (statuses.isEmpty()) {
                            logger.info(screenName + " total : " + timeline.size());
                            break;
                        }
                        timeline.addAll(statuses);
                    }
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
        }

    }

    public List<String> getUsers() {
        return screenNames;
    }
}
