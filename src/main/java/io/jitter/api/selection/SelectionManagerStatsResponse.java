package io.jitter.api.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.api.ResponseHeader;
import io.jitter.api.shards.ShardStatsResponse;

public class SelectionManagerStatsResponse {

    private ResponseHeader responseHeader;
    private ShardStatsResponse response;

    public SelectionManagerStatsResponse() {
        // Jackson deserialization
    }

    public SelectionManagerStatsResponse(ResponseHeader responseHeader, ShardStatsResponse response) {
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
