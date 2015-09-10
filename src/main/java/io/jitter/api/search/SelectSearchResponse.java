package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.api.ResponseHeader;

public class SelectSearchResponse {

    private ResponseHeader responseHeader;
    private SelectDocumentsResponse response;

    public SelectSearchResponse() {
        // Jackson deserialization
    }

    public SelectSearchResponse(ResponseHeader responseHeader, SelectDocumentsResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public SelectDocumentsResponse getResponse() {
        return response;
    }

}
