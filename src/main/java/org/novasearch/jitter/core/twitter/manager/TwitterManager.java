package org.novasearch.jitter.core.twitter.manager;

import cc.twittertools.index.IndexStatuses;
import io.dropwizard.lifecycle.Managed;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.Status;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TwitterManager implements Managed {
    final static Logger logger = LoggerFactory.getLogger(TwitterManager.class);

    private static final int MAX_USERS_LOOKUP = 100;
    private static final int MAX_STATUSES_REQUEST = 200;

    @SuppressWarnings("FieldCanBeLocal")
    private final String database;
    @SuppressWarnings("FieldCanBeLocal")
    private final String collection;
    private final List<String> screenNames;

    private final Map<String, User> usersMap;
    private final Map<String, UserTimeline> userTimelines;

    // The factory instance is re-useable and thread safe.
    private final Twitter twitter = TwitterFactory.getSingleton();

    public TwitterManager(String database, String collection, List<String> screenNames) {
        this.database = database;
        this.collection = collection;
        this.screenNames = screenNames;
        this.usersMap = new LinkedHashMap<>();
        this.userTimelines = new LinkedHashMap<>();
    }

    @Override
    public void start() throws Exception {
//        lookupUsers();
    }

    @Override
    public void stop() throws Exception {

    }

    public List<String> getUsers() {
        return screenNames;
    }

    public UserTimeline getUserTimeline(String screenName) {
        return userTimelines.get(screenName);
    }

    public void lookupUsers() {
        int remaining = screenNames.size();
        logger.info(remaining + " total users");

        int i = 0;
        do {
            int reqSize = Math.min(remaining, MAX_USERS_LOOKUP);

            List<String> var = screenNames.subList(i, i + reqSize);
            String[] names = var.toArray(new String[var.size()]);
            ResponseList<User> userResponseList;
            try {
                logger.info(reqSize + " users info requested");
                userResponseList = twitter.lookupUsers(names);
                for (User user : userResponseList) {
                    logger.info("Got info for " + user.getScreenName() + " : " + user.getName() + " : " + user.getStatusesCount());
                    usersMap.put(user.getScreenName().toLowerCase(), user);
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            }
            remaining -= reqSize;
            i += reqSize;
        } while (remaining > 0);
    }

    private void fetchTimeline(String screenName) {
        User user = usersMap.get(screenName);

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
                for (; ; page++) {
                    paging.setPage(page);
                    logger.info(screenName + " page: " + page);
                    ResponseList<Status> statuses = twitter.getUserTimeline(user.getId(), paging);
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

    public void archive() {
        lookupUsers();
        for (String screenName : screenNames) {
            User user = usersMap.get(screenName);
            if (user == null)
                logger.warn("Failed to lookup " + screenName);

            fetchTimeline(screenName);
        }
    }

//    public void loadCollection() throws IOException {
//        JsonStatusCorpusReader statusReader = new JsonStatusCorpusReader(new File(collection));
//
//        UserTimeline timeline;
//        if (userTimelines.get(screenName) != null) {
//            timeline = userTimelines.get(screenName);
//        } else {
//            timeline = new org.novasearch.jitter.core.twitter.archiver.UserTimeline(screenName);
//            userTimelines.put(screenName, timeline);
//        }
//
//        cc.twittertools.corpus.data.Status status;
//        int cnt = 0;
//        while (true) {
//            status = statusReader.next();
//            if (status == null) {
//                break;
//            }
//            if (status.getId() <= timeline.getLatestId()) {
//                continue;
//            }
//            timeline.add(status);
//            cnt++;
//        }
//        statusReader.close();
//        if (cnt > 0) {
//            logger.info("loaded " + cnt);
//        }
//    }

    public void index(String index, boolean removeDuplicates) throws IOException {
        File indexPath = new File(index);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(org.apache.lucene.util.Version.LUCENE_43, IndexStatuses.ANALYZER);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);

        int cnt = 0;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String screenName : getUsers()) {
                UserTimeline userTimeline = getUserTimeline(screenName);
                if (userTimeline == null)
                    break;
                LinkedHashMap<Long, Status> statuses = userTimeline.getStatuses();
                if (userTimeline.getStatuses() == null)
                    break;
                for (Status status : statuses.values()) {
                    cnt++;
                    org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                    doc.add(new LongField(IndexStatuses.StatusField.ID.name, status.getId(), Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getCreatedAt().getTime(), Field.Store.YES));
                    doc.add(new TextField(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Field.Store.YES));

                    doc.add(new Field(IndexStatuses.StatusField.TEXT.name, status.getText(), textOptions));

                    doc.add(new IntField(IndexStatuses.StatusField.FRIENDS_COUNT.name, status.getUser().getFollowersCount(), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name, status.getUser().getFriendsCount(), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount(), Field.Store.YES));

                    long inReplyToStatusId = status.getInReplyToStatusId();
                    if (inReplyToStatusId > 0) {
                        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
                        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
                    }

                    String lang = status.getLang();
                    if (!lang.equals("unknown")) {
                        doc.add(new TextField(IndexStatuses.StatusField.LANG.name, status.getLang(), Field.Store.YES));
                    }

                    if (status.isRetweet()) {
                        long retweetStatusId = status.getRetweetedStatus().getId();
                        if (retweetStatusId > 0) {
                            doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
                            doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_USER_ID.name, status.getRetweetedStatus().getUser().getId(), Field.Store.YES));
                            doc.add(new IntField(IndexStatuses.StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Field.Store.YES));
                            if (status.getRetweetCount() < 0 || status.getRetweetedStatus().getId() < 0) {
                                logger.warn("Error parsing retweet fields of " + status.getId());
                            }
                        }
                    }

                    writer.addDocument(doc);
                    if (cnt % 1000 == 0) {
                        logger.debug(cnt + " statuses indexed");
                    }
                }
                logger.info(String.format("Total of %s statuses added", cnt));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dir.close();
        }
    }
}
