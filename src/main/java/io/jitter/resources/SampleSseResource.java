package io.jitter.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.media.sse.SseFeature;
import org.slf4j.Logger;
import twitter4j.*;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
@Path("/sample")
//@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SampleSseResource implements StatusListener, RawStreamListener {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(SampleSseResource.class);

    private final AtomicLong counter;
    private final SseBroadcaster broadcaster;
    private final ObjectWriter objectWriter;

    public SampleSseResource() {
        counter = new AtomicLong();
        broadcaster = new SseBroadcaster();
        objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
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

    }

    @Override
    public void onException(Exception ex) {

    }

    @Override
    public void onStatus(Status status) {
        final GeoLocation geoLocation = status.getGeoLocation();
        if (geoLocation != null) {
            String json;
            try {
                json = objectWriter.writeValueAsString(status);
                StringBuilder sb = new StringBuilder();
                sb.append(json.substring(0, json.length()-2)).append(",\n");
                sb.append("  \"id_str\" : \"").append(String.valueOf(status.getId())).append("\"\n}");
                json = sb.toString();
                broadcastMessage("status", json);
            } catch (JsonProcessingException e) {
                logger.error(e.getMessage());
            }
        }
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
