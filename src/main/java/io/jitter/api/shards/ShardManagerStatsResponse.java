package io.jitter.api.shards;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.api.ResponseHeader;

public class ShardManagerStatsResponse {

    private ResponseHeader responseHeader;
    private ShardStatsResponse response;

    public ShardManagerStatsResponse() {
        // Jackson deserialization
    }

    public ShardManagerStatsResponse(ResponseHeader responseHeader, ShardStatsResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public ShardStatsResponse getResponse() {
        return response;
    }
}
