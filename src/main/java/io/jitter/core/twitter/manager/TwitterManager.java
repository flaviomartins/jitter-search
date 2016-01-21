package io.jitter.core.twitter.manager;

import cc.twittertools.corpus.data.*;
import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.dropwizard.lifecycle.Managed;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.Status;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TwitterManager implements Managed {

    private final static Logger logger = LoggerFactory.getLogger(TwitterManager.class);

    private static final Analyzer analyzer = IndexStatuses.ANALYZER;

    private static final int MAX_USERS_LOOKUP = 100;
    private static final int MAX_STATUSES_REQUEST = 200;

    @SuppressWarnings("FieldCanBeLocal")
    private final String databasePath;
    private final String collectionPath;
    private final ImmutableSortedSet<String> screenNames;

    private final Map<String, User> usersMap;
    private final Map<String, UserTimeline> userTimelines;

    // The factory instance is re-usable and thread safe.
    private final Twitter twitter = TwitterFactory.getSingleton();

    public TwitterManager(String databasePath, String collectionPath, Set<String> screenNames) {
        this.databasePath = databasePath;
        this.collectionPath = collectionPath;
        this.screenNames = new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER).addAll(screenNames).build();
        this.usersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.userTimelines = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    @Override
    public void start() throws Exception {
//        lookupUsers();
    }

    @Override
    public void stop() throws Exception {

    }

    public ImmutableSortedSet<String> getUsers() {
        return screenNames;
    }

    private UserTimeline getUserTimeline(String screenName) {
        return userTimelines.get(screenName);
    }

    private void lookupUsers() {
        int remaining = screenNames.size();
        logger.info(remaining + " total users");

        ImmutableList<String> users = ImmutableList.copyOf(screenNames);
        int i = 0;
        do {
            int reqSize = Math.min(remaining, MAX_USERS_LOOKUP);

            List<String> var = users.subList(i, i + reqSize);
            String[] names = var.toArray(new String[var.size()]);
            ResponseList<User> userResponseList;
            try {
                logger.info("{} users info requested", reqSize);
                userResponseList = twitter.lookupUsers(names);
                for (User user : userResponseList) {
                    logger.info("Got info for " + user.getScreenName() + " : " + user.getName() + " : " + user.getStatusesCount());
                    usersMap.put(user.getScreenName(), user);
                }
            } catch (TwitterException e) {
                logger.error("{}", e.getMessage());
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
            logger.error("{}", e.getMessage());
        }
    }

    public void archive() {
        lookupUsers();
        for (String screenName : screenNames) {
            User user = usersMap.get(screenName);
            if (user == null)
                logger.warn("Failed to lookup {}", screenName);

            fetchTimeline(screenName);
        }
    }

    public void index(String indexPath, boolean removeDuplicates) throws IOException {
        long startTime = System.currentTimeMillis();
        File file = new File(collectionPath);
        if (!file.exists()) {
            logger.error("Error: " + file + " does not exist!");
            return;
        }

        StatusStream stream = new JsonStatusCorpusReader(file);

        Directory dir = FSDirectory.open(new File(indexPath));
        
        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        fieldAnalyzers.put(IndexStatuses.StatusField.SCREEN_NAME.name, new SimpleAnalyzer());
        PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(analyzer, fieldAnalyzers);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_3, perFieldAnalyzerWrapper);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);
        
        final FieldType screenNameOptions = new FieldType();
        screenNameOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        screenNameOptions.setStored(true);
        screenNameOptions.setTokenized(true);

        int cnt = 0;
        cc.twittertools.corpus.data.Status status;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            while ((status = stream.next()) != null) {
                if (status.getText() == null) {
                    continue;
                }

                cnt++;
                Document doc = new Document();
                doc.add(new LongField(IndexStatuses.StatusField.ID.name, status.getId(), Field.Store.YES));
                doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getEpoch(), Field.Store.YES));
                doc.add(new Field(IndexStatuses.StatusField.SCREEN_NAME.name, status.getScreenname(), screenNameOptions));

                doc.add(new Field(IndexStatuses.StatusField.TEXT.name, status.getText(), textOptions));

                doc.add(new IntField(IndexStatuses.StatusField.FRIENDS_COUNT.name, status.getFollowersCount(), Field.Store.YES));
                doc.add(new IntField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name, status.getFriendsCount(), Field.Store.YES));
                doc.add(new IntField(IndexStatuses.StatusField.STATUSES_COUNT.name, status.getStatusesCount(), Field.Store.YES));

                long inReplyToStatusId = status.getInReplyToStatusId();
                if (inReplyToStatusId > 0) {
                    doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
                }

                String lang = status.getLang();
                if (!lang.equals("unknown")) {
                    doc.add(new TextField(IndexStatuses.StatusField.LANG.name, status.getLang(), Field.Store.YES));
                }

                long retweetStatusId = status.getRetweetedStatusId();
                if (retweetStatusId > 0) {
                    doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_USER_ID.name, status.getRetweetedUserId(), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Field.Store.YES));
                    if (status.getRetweetCount() < 0 || status.getRetweetedStatusId() < 0) {
                        logger.warn("Error parsing retweet fields of " + status.getId());
                    }
                }

                writer.addDocument(doc);
                if (cnt % 1000 == 0) {
                    logger.debug(cnt + " statuses indexed");
                }
            }

            logger.info(String.format(Locale.ENGLISH, "Total of %s statuses added", cnt));
            logger.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        } finally {
            dir.close();
            stream.close();
        }
    }

    public void indexLive(String indexPath, boolean removeDuplicates) throws IOException {
        Directory dir = FSDirectory.open(new File(indexPath));

        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        fieldAnalyzers.put(IndexStatuses.StatusField.SCREEN_NAME.name, new SimpleAnalyzer());
        PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(analyzer, fieldAnalyzers);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_3, perFieldAnalyzerWrapper);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);

        final FieldType screenNameOptions = new FieldType();
        screenNameOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        screenNameOptions.setStored(true);
        screenNameOptions.setTokenized(true);
        
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
                    doc.add(new Field(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), screenNameOptions));

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
                logger.info(String.format(Locale.ENGLISH, "Total of %s statuses added", cnt));
            }

        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        } finally {
            dir.close();
        }
    }
}
