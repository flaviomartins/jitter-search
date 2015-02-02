package org.novasearch.jitter.api.rs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.novasearch.jitter.api.ResponseHeader;
import org.novasearch.jitter.api.search.DocumentsResponse;

public class ResourceSelectionResponse {

    private ResponseHeader responseHeader;
    private ResourceSelectionDocumentsResponse response;

    public ResourceSelectionResponse() {
        // Jackson deserialization
    }

    public ResourceSelectionResponse(ResponseHeader responseHeader, ResourceSelectionDocumentsResponse response) {
        this.responseHeader = responseHeader;
        this.response = response;
    }

    @JsonProperty
    public ResponseHeader getResponseHeader() {
        return responseHeader;
    }

    @JsonProperty
    public ResourceSelectionDocumentsResponse getResponse() {
        return response;
    }

}
