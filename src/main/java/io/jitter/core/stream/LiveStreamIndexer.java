package io.jitter.core.stream;

import cc.twittertools.index.IndexStatuses;
import io.dropwizard.lifecycle.Managed;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LiveStreamIndexer implements Managed, StatusListener, UserStreamListener {

    private static final Logger logger = LoggerFactory.getLogger(LiveStreamIndexer.class);

    private static final Analyzer analyzer = IndexStatuses.ANALYZER;

    private final AtomicLong counter;
    private final String indexPath;
    private final int commitEvery;
    private final boolean stringField;

    private final FieldType textOptions;
    private final IndexWriter writer;

    public LiveStreamIndexer(String indexPath, int commitEvery, boolean stringField) throws IOException {
        counter = new AtomicLong();
        this.indexPath = indexPath;
        this.commitEvery = commitEvery;
        this.stringField = stringField;

        Directory dir = FSDirectory.open(new File(indexPath));
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);

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
            counter.incrementAndGet();
            Document doc = new Document();
            long id = status.getId();
            doc.add(new LongField(IndexStatuses.StatusField.ID.name, id, Field.Store.YES));
            doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getCreatedAt().getTime() / 1000L, Field.Store.YES));
            if (stringField) {
                doc.add(new StringField(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Field.Store.YES));
            } else {
                doc.add(new TextField(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Field.Store.YES));

            }
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
                doc.add(new TextField(IndexStatuses.StatusField.LANG.name, lang, Field.Store.YES));
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

            writer.addDocument(doc);
            if (counter.get() % commitEvery == 0) {
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
