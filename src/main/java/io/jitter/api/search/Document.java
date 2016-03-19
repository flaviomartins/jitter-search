package io.jitter.api.search;

import cc.twittertools.index.IndexStatuses;
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

    public Document(org.apache.lucene.document.Document hit) {
        this.id = (Long) hit.getField(IndexStatuses.StatusField.ID.name).numericValue();
        this.screen_name = hit.get(IndexStatuses.StatusField.SCREEN_NAME.name);
        this.epoch = (Long) hit.getField(IndexStatuses.StatusField.EPOCH.name).numericValue();
        this.text = hit.get(IndexStatuses.StatusField.TEXT.name);

        this.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
        this.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();

        if (hit.get(IndexStatuses.StatusField.LANG.name) != null) {
            this.lang = hit.get(IndexStatuses.StatusField.LANG.name);
        }

        if (hit.get(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name) != null) {
            this.in_reply_to_status_id = (Long) hit.getField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name) != null) {
            this.in_reply_to_user_id = (Long) hit.getField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name) != null) {
            this.retweeted_status_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.RETWEETED_USER_ID.name) != null) {
            this.retweeted_user_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_USER_ID.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.RETWEET_COUNT.name) != null) {
            this.retweeted_count = (Integer) hit.getField(IndexStatuses.StatusField.RETWEET_COUNT.name).numericValue();
        }
    }
}
