package io.jitter.resources;

import com.twitter.hbc.twitter4j.handler.UserstreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.media.sse.SseFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
@Path("/timeline")
//@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TimelineSseResource implements UserstreamHandler, RawStreamListener {

    private static final Logger logger = LoggerFactory.getLogger(TimelineSseResource.class);

    private final AtomicLong counter;
    private final SseBroadcaster broadcaster;

    public TimelineSseResource() {
        counter = new AtomicLong();
        broadcaster = new SseBroadcaster();
    }

//    @Produces(MediaType.TEXT_PLAIN)
    private void broadcastMessage(String name, String msg) {
        final OutboundEvent.Builder eventBuilder = new OutboundEvent.Builder();
        final OutboundEvent event = eventBuilder.name(name)
                .mediaType(MediaType.TEXT_PLAIN_TYPE)
                .id(String.valueOf(counter.incrementAndGet()))
                .data(String.class, msg)
                .build();
        broadcaster.broadcast(event);
    }

    @GET
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput listenToBroadcast() {
        final EventOutput eventOutput = new EventOutput();
        broadcaster.add(eventOutput);
        return eventOutput;
    }

    @Override
    public void onMessage(String rawString) {
        broadcastMessage("status", rawString);
    }

    @Override
    public void onException(Exception ex) {

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
    public void onDisconnectMessage(DisconnectMessage disconnectMessage) {

    }

    @Override
    public void onUnfollow(User source, User unfollowedUser) {

    }

    @Override
    public void onStallWarningMessage(StallWarningMessage warning) {

    }

    @Override
    public void onUnknownMessageType(String msg) {

    }

    @Override
    public void onRetweet(User source, User target, Status retweetedStatus) {

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
}
