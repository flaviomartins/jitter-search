package org.novasearch.jitter.api.reputation;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.novasearch.jitter.api.ResponseHeader;

public class ReputationResponse {

    private ResponseHeader responseHeader;
    private InnerReputationResponse response;

    public ReputationResponse() {
        // Jackson deserialization
    }

    public ReputationResponse(ResponseHeader responseHeader, InnerReputationResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public InnerReputationResponse getResponse() {
        return response;
    }

}
