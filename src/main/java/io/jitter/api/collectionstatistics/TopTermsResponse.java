package io.jitter.api.collectionstatistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.api.ResponseHeader;

public class TopTermsResponse {

    ResponseHeader responseHeader;
    private TermsResponse response;

    public TopTermsResponse() {
        // Jackson deserialization
    }

    public TopTermsResponse(ResponseHeader responseHeader, TermsResponse response) {
        this.responseHeader = responseHeader;

        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public TermsResponse getResponse() {
        return response;
    }
}
