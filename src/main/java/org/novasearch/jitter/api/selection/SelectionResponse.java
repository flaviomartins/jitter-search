package org.novasearch.jitter.api.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.novasearch.jitter.api.ResponseHeader;

public class SelectionResponse {

    private ResponseHeader responseHeader;
    private SelectionDocumentsResponse response;

    public SelectionResponse() {
        // Jackson deserialization
    }

    public SelectionResponse(ResponseHeader responseHeader, SelectionDocumentsResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public SelectionDocumentsResponse getResponse() {
        return response;
    }

}
