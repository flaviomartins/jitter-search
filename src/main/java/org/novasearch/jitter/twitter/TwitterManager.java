package org.novasearch.jitter.twitter;

import com.fasterxml.jackson.core.util.TextBuffer;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.LinkedHashMap;
import java.util.List;

public class TwitterManager implements Managed {
    final static Logger logger = LoggerFactory.getLogger(TwitterManager.class);

    private static final int MAX_USERS_LOOKUP = 100;
    private static final int MAX_STATUSES_REQUEST = 200;

    private LinkedHashMap<String, User> usersMap;
    private List<String> screenNames;

    // The factory instance is re-useable and thread safe.
    private Twitter twitter = TwitterFactory.getSingleton();

    public TwitterManager(List<String> screenNames) {
        this.screenNames = screenNames;
        this.usersMap = new LinkedHashMap<>();
    }

    @Override
    public void start() throws Exception {
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

    @Override
    public void stop() throws Exception {

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

    public void archive() {

    }

    public List<String> getUsers() {
        return screenNames;
    }
}
