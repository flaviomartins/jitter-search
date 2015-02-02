package org.novasearch.jitter.twitter;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.LinkedHashMap;
import java.util.List;

public class TwitterUserTimelinesManager {
    final static Logger logger = LoggerFactory.getLogger(TwitterUserTimelinesManager.class);

    private static final int MAX_USERS_LOOKUP = 100;
    private static final int MAX_STATUSES_REQUEST = 200;

    private LinkedHashMap<String, User> usersMap;
    private List<String> screenNames;

    // The factory instance is re-useable and thread safe.
    private Twitter twitter = TwitterFactory.getSingleton();

    public TwitterUserTimelinesManager(List<String> screenNames) {
        this.screenNames = screenNames;
        this.usersMap = new LinkedHashMap<>();
    }

    public void start() {
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

//        for (User user: usersMap.values()) {
//            getUserTimeline(user.getScreenName());
//        }
    }

    public List<Status> getUserTimeline(String screenName) {
        List<Status> all = Lists.newArrayList();
        User user = usersMap.get(screenName);
        try {
            if (user.getStatus() != null) {
                for (int page = 1;;page++) {
                    List<Status> statuses = null;
                        statuses = twitter.getUserTimeline(user.getId(), new Paging(page, MAX_STATUSES_REQUEST));
                    if (statuses.isEmpty())
                        break;
                    all.addAll(statuses);
                }
            }
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        return all;
    }

}
