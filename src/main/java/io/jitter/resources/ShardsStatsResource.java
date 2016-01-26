package io.jitter.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.jitter.api.ResponseHeader;
import io.jitter.api.shards.ShardManagerStatsResponse;
import io.jitter.api.shards.ShardStatsResponse;
import io.jitter.core.shards.ShardStats;
import io.jitter.core.shards.ShardsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

@Path("/shards/stats")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class ShardsStatsResource {
    private static final Logger logger = LoggerFactory.getLogger(TopTermsResource.class);

    private final AtomicLong counter;
    private final ShardsManager shardsManager;

    public ShardsStatsResource(ShardsManager shardsManager) throws IOException {
        Preconditions.checkNotNull(shardsManager);

        counter = new AtomicLong();
        this.shardsManager = shardsManager;
    }

    @GET
    @Timed
    public ShardManagerStatsResponse top(@Context UriInfo uriInfo) throws Exception {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        long startTime = System.currentTimeMillis();

        ShardStats collectionsShardStats = shardsManager.getCollectionsShardStats();
        ShardStats topicsShardStats = shardsManager.getTopicsShardStats();

        long endTime = System.currentTimeMillis();

        logger.info(String.format(Locale.ENGLISH, "%4dms selection manager stats", (endTime - startTime)));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        ShardStatsResponse shardStatsResponse = new ShardStatsResponse(collectionsShardStats, topicsShardStats);
        return new ShardManagerStatsResponse(responseHeader, shardStatsResponse);
    }
}