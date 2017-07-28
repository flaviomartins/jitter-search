package io.jitter.core.stream;

import io.dropwizard.lifecycle.Managed;
import io.jitter.core.analysis.LowercaseKeywordAnalyzer;
import io.jitter.core.analysis.TweetAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static cc.twittertools.index.IndexStatuses.*;
import static org.apache.lucene.document.Field.*;

public class LiveStreamIndexer implements Managed, StatusListener, UserStreamListener {

    private static final Logger logger = LoggerFactory.getLogger(LiveStreamIndexer.class);

    private static final Analyzer ANALYZER = new TweetAnalyzer();

    private final AtomicLong counter;
    private final String indexPath;
    private final int commitEvery;
    private final boolean stringField;

    private final FieldType textOptions;
    private final FieldType screenNameOptions;
    private final IndexWriter writer;

    public LiveStreamIndexer(String indexPath, int commitEvery, boolean stringField) throws IOException {
        counter = new AtomicLong();
        this.indexPath = indexPath;
        this.commitEvery = commitEvery;
        this.stringField = stringField;

        Directory dir = FSDirectory.open(Paths.get(indexPath));

        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        fieldAnalyzers.put(StatusField.SCREEN_NAME.name, new LowercaseKeywordAnalyzer());
        PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(ANALYZER, fieldAnalyzers);

        IndexWriterConfig config = new IndexWriterConfig(perFieldAnalyzerWrapper);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        textOptions = new FieldType();
        textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);
        textOptions.setStoreTermVectors(true);

        screenNameOptions = new FieldType();
        screenNameOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        screenNameOptions.setStored(true);
        screenNameOptions.setTokenized(true);

        writer = new IndexWriter(dir, config);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

    private void index(Status status) {
        if (writer == null) return;
        try {
            Document doc = new Document();
            long id = status.getId();
            doc.add(new LongField(StatusField.ID.name, id, Store.YES));
            doc.add(new LongField(StatusField.EPOCH.name, status.getCreatedAt().getTime() / 1000L, Store.YES));
            if (stringField) {
                doc.add(new StringField(StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Store.YES));
            } else {
                doc.add(new Field(StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), screenNameOptions));

            }
            doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

            doc.add(new IntField(StatusField.FRIENDS_COUNT.name, status.getUser().getFriendsCount(), Store.YES));
            doc.add(new IntField(StatusField.FOLLOWERS_COUNT.name, status.getUser().getFollowersCount(), Store.YES));
            doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount(), Store.YES));

            long inReplyToStatusId = status.getInReplyToStatusId();
            if (inReplyToStatusId > 0) {
                doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Store.YES));
                doc.add(new LongField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Store.YES));
            }

            String lang = status.getLang();
            if (lang != null) {
                doc.add(new TextField(StatusField.LANG.name, lang, Store.YES));
            }

            int retweetedStatusRetweetCount = -1;
            Status retweetedStatus = status.getRetweetedStatus();
            if (retweetedStatus != null) {
                retweetedStatusRetweetCount = retweetedStatus.getRetweetCount();
                doc.add(new LongField(StatusField.RETWEETED_STATUS_ID.name, retweetedStatus.getId(), Store.YES));
                doc.add(new LongField(StatusField.RETWEETED_USER_ID.name, retweetedStatus.getUser().getId(), Store.YES));
            }

            int retweetCount = status.getRetweetCount();
            doc.add(new IntField(StatusField.RETWEET_COUNT.name, Math.max(retweetCount, retweetedStatusRetweetCount), Store.YES));

            writer.addDocument(doc);
            if (counter.incrementAndGet() % commitEvery == 0) {
                logger.debug("{} {} statuses indexed", indexPath, counter.get());
                writer.commit();
            }
        } catch (AlreadyClosedException e) {
            // do nothing
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void onDeletionNotice(long directMessageId, long userId) {

    }

    @Override
    public void onFriendList(long[] friendIds) {

    }

    @Override
    public void onFavorite(User source, User target, Status favoritedStatus) {

    }

    @Override
    public void onUnfavorite(User source, User target, Status unfavoritedStatus) {

    }

    @Override
    public void onFollow(User source, User followedUser) {

    }

    @Override
    public void onUnfollow(User source, User unfollowedUser) {

    }

    @Override
    public void onDirectMessage(DirectMessage directMessage) {

    }

    @Override
    public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {

    }

    @Override
    public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {

    }

    @Override
    public void onUserListSubscription(User subscriber, User listOwner, UserList list) {

    }

    @Override
    public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {

    }

    @Override
    public void onUserListCreation(User listOwner, UserList list) {

    }

    @Override
    public void onUserListUpdate(User listOwner, UserList list) {

    }

    @Override
    public void onUserListDeletion(User listOwner, UserList list) {

    }

    @Override
    public void onUserProfileUpdate(User updatedUser) {

    }

    @Override
    public void onUserSuspension(long suspendedUser) {

    }

    @Override
    public void onUserDeletion(long deletedUser) {

    }

    @Override
    public void onBlock(User source, User blockedUser) {

    }

    @Override
    public void onUnblock(User source, User unblockedUser) {

    }

    @Override
    public void onRetweetedRetweet(User user, User user1, Status status) {

    }

    @Override
    public void onFavoritedRetweet(User user, User user1, Status status) {

    }

    @Override
    public void onQuotedTweet(User user, User user1, Status status) {

    }

    @Override
    public void onStatus(Status status) {
        index(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {

    }

}
