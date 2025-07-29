package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.jitter.api.ResponseHeader;
import io.jitter.api.selection.SelectionManagerStatsResponse;
import io.jitter.api.shards.ShardStatsResponse;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.shards.ShardStats;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

@Path("/selection/stats")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class SelectionStatsResource {
    private static final Logger logger = LoggerFactory.getLogger(TopTermsResource.class);

    private final AtomicLong counter;
    private final SelectionManager selectionManager;

    public SelectionStatsResource(SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.selectionManager = selectionManager;
    }

    @GET
    @Timed
    public SelectionManagerStatsResponse top(@Context UriInfo uriInfo) throws Exception {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        long startTime = System.currentTimeMillis();

        ShardStats csiStats = selectionManager.getCsiStats();
        ShardStats shardStats = selectionManager.getShardStats();

        long endTime = System.currentTimeMillis();

        logger.info(String.format(Locale.ENGLISH, "%4dms selection manager stats", (endTime - startTime)));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        ShardStatsResponse shardStatsResponse = new ShardStatsResponse(csiStats, shardStats);
        return new SelectionManagerStatsResponse(responseHeader, shardStatsResponse);
    }
}