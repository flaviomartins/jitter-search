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
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TwitterManager implements Managed {

    private final static Logger logger = LoggerFactory.getLogger(TwitterManager.class);

    private static final int MAX_USERS_LOOKUP = 100;
    private static final int MAX_STATUSES_REQUEST = 200;

    private final ImmutableSortedSet<String> screenNames;

    private final Map<String, User> usersMap;
    private final Map<String, UserTimeline> userTimelines;

    // The factory instance is re-usable and thread safe.
    private final Twitter twitter = TwitterFactory.getSingleton();

    public TwitterManager(Set<String> screenNames) {
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

    @SuppressWarnings("UnusedParameters")
    public void index(String collection, String indexPath, Analyzer analyzer) throws IOException {
        long startTime = System.currentTimeMillis();
        File file = new File(collection);
        if (!file.isDirectory()) {
            logger.error("Error: " + file + " does not exist!");
            return;
        }

        StatusStream stream = new JsonStatusCorpusReader(file);

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        
        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        fieldAnalyzers.put(IndexStatuses.StatusField.SCREEN_NAME.name, new SimpleAnalyzer());
        PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(analyzer, fieldAnalyzers);

        IndexWriterConfig config = new IndexWriterConfig(perFieldAnalyzerWrapper);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);
        textOptions.setStoreTermVectors(true);
        
        final FieldType screenNameOptions = new FieldType();
        screenNameOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        screenNameOptions.setStored(true);
        screenNameOptions.setTokenized(true);

        AtomicLong counter = new AtomicLong();
        Status status;
        int commitEvery = 1000;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            while ((status = stream.next()) != null) {
                if (status.getText() == null) {
                    continue;
                }
                Document doc = new Document();
                long id = status.getId();
                doc.add(new LongField(IndexStatuses.StatusField.ID.name, id, Field.Store.YES));
                doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getCreatedAt().getTime() / 1000L, Field.Store.YES));
                doc.add(new Field(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), screenNameOptions));

                doc.add(new Field(IndexStatuses.StatusField.TEXT.name, status.getText(), textOptions));

                doc.add(new IntField(IndexStatuses.StatusField.FRIENDS_COUNT.name, status.getUser().getFriendsCount(), Field.Store.YES));
                doc.add(new IntField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name, status.getUser().getFollowersCount(), Field.Store.YES));
                doc.add(new IntField(IndexStatuses.StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount(), Field.Store.YES));

                long inReplyToStatusId = status.getInReplyToStatusId();
                if (inReplyToStatusId > 0) {
                    doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
                }

                String lang = status.getLang();
                if (lang != null && !"unknown".equals(lang)) {
                    doc.add(new TextField(IndexStatuses.StatusField.LANG.name, status.getLang(), Field.Store.YES));
                }

                Status retweetedStatus = status.getRetweetedStatus();
                if (retweetedStatus != null) {
                    doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name, status.getRetweetedStatus().getId(), Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_USER_ID.name, status.getRetweetedStatus().getUser().getId(), Field.Store.YES));
                    int retweetCount = status.getRetweetCount();
                    doc.add(new IntField(IndexStatuses.StatusField.RETWEET_COUNT.name, retweetCount, Field.Store.YES));
                    if (retweetCount < 0) {
                        logger.warn("Error parsing retweet fields of {}", id);
                    }
                }

                Term delTerm = new Term(IndexStatuses.StatusField.ID.name, Long.toString(status.getId()));
                writer.updateDocument(delTerm, doc);
                if (counter.incrementAndGet() % commitEvery == 0) {
                    logger.debug("{} {} statuses indexed", indexPath, counter.get());
                    writer.commit();
                }
            }

            logger.info(String.format(Locale.ENGLISH, "Total of %s statuses added", counter.get()));
            logger.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        } finally {
            dir.close();
            stream.close();
        }
    }

}
