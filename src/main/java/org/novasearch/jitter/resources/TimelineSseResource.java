package org.novasearch.jitter.resources;

import org.apache.log4j.Logger;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.media.sse.SseFeature;
import org.novasearch.jitter.core.stream.StreamEventListener;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
@Path("/timeline")
//@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class TimelineSseResource implements StreamEventListener {
    private static final Logger logger = Logger.getLogger(TimelineSseResource.class);

    private final AtomicLong counter;
    private final SseBroadcaster broadcaster;

    public TimelineSseResource() {
        counter = new AtomicLong();
        broadcaster = new SseBroadcaster();
    }

//    @Produces(MediaType.TEXT_PLAIN)
    public void broadcastMessage(String msg) {
        final OutboundEvent.Builder eventBuilder = new OutboundEvent.Builder();
        final OutboundEvent event = eventBuilder.name("message")
                .mediaType(MediaType.TEXT_PLAIN_TYPE)
                .id(String.valueOf(counter.incrementAndGet()))
                .data(String.class, msg)
                .build();
        broadcaster.broadcast(event);

        logger.info("Message '" + msg + "' has been broadcast.");
    }

    @GET
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput listenToBroadcast() {
        final EventOutput eventOutput = new EventOutput();
        broadcaster.add(eventOutput);
        return eventOutput;
    }

    @Override
    public void onMsg(String msg) {
        logger.info(msg);
        broadcastMessage(msg);
    }
}
