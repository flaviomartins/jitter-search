package io.jitter.api.search;

import cc.twittertools.thrift.gen.TResult;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.jitter.core.twittertools.api.TResultWrapper;

@JsonIgnoreProperties(ignoreUnknown = true, value = {"setId", "setRsv", "setScreen_name", "setEpoch", "setText",
        "setFollowers_count", "setStatuses_count", "setLang", "setIn_reply_to_status_id", "setIn_reply_to_user_id",
        "setRetweeted_status_id", "setRetweeted_user_id", "setRetweeted_count", "features", "properties", "dataPoint"})
public class Document extends TResultWrapper {

    public Document() {
        super();
    }

    public Document(Document other) {
        super(other);
    }

    public Document(TResult other) {
        super(other);
    }

    public Document(TResultWrapper other) {
        super(other);
    }
}
