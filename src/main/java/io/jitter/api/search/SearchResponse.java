package io.jitter.api.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jitter.api.ResponseHeader;

public class SearchResponse {

    private ResponseHeader responseHeader;
    private DocumentsResponse response;

    public SearchResponse() {
        // Jackson deserialization
    }

    public SearchResponse(ResponseHeader responseHeader, DocumentsResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public DocumentsResponse getResponse() {
        return response;
    }

}
