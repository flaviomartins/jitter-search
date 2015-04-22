package org.novasearch.jitter.resources;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.media.sse.SseFeature;
import org.slf4j.Logger;
import twitter4j.RawStreamListener;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
@Path("/sample")
//@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SampleSseResource implements RawStreamListener {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(SampleSseResource.class);

    private final AtomicLong counter;
    private final SseBroadcaster broadcaster;

    public SampleSseResource() {
        counter = new AtomicLong();
        broadcaster = new SseBroadcaster();
    }

//    @Produces(MediaType.TEXT_PLAIN)
    public void broadcastMessage(String name, String msg) {
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
        broadcastMessage("message", rawString);
    }

    @Override
    public void onException(Exception ex) {

    }
}
