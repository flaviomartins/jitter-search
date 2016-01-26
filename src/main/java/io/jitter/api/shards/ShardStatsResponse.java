package io.jitter.api.shards;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.jitter.core.shards.ShardStats;

@JsonPropertyOrder({"topics", "collections"})
public class ShardStatsResponse {

    private ShardStats collections;
    private ShardStats topics;

    public ShardStatsResponse() {
        // Jackson deserialization
    }
            
    public ShardStatsResponse(ShardStats collections, ShardStats topics) {
        this.collections = collections;
        this.topics = topics;
    }

    public ShardStats getCollections() {
        return collections;
    }

    public ShardStats getTopics() {
        return topics;
    }
}
