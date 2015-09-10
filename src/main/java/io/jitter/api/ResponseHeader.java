package io.jitter.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.MultivaluedMap;

public class ResponseHeader {

    private long id;
    private int status;
    private long QTime;
    private MultivaluedMap<String, String> params;

    public ResponseHeader() {
        // Jackson deserialization
    }

    public ResponseHeader(long id, int status, long QTime, MultivaluedMap<String, String> params) {
        this.id = id;
        this.status = status;
        this.QTime = QTime;
        this.params = params;
    }

    @JsonProperty
    public long getId() {
        return id;
    }

    @JsonProperty
    public int getStatus() {
        return status;
    }

    @JsonProperty
    public long getQTime() {
        return QTime;
    }

    @JsonProperty
    public MultivaluedMap<String, String> getParams() {
        return params;
    }
}
