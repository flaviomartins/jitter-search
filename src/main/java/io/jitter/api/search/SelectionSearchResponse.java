package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.api.ResponseHeader;

public class SelectionSearchResponse {

    private ResponseHeader responseHeader;
    private SelectionSearchDocumentsResponse response;

    public SelectionSearchResponse() {
        // Jackson deserialization
    }

    public SelectionSearchResponse(ResponseHeader responseHeader, SelectionSearchDocumentsResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public SelectionSearchDocumentsResponse getResponse() {
        return response;
    }

}
