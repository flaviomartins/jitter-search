package io.jitter.api.search;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.thrift.gen.TResult;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.jitter.core.wikipedia.WikipediaManager;

@JsonIgnoreProperties(ignoreUnknown = true, value = {"docVector", "features"})
public class StatusDocument extends AbstractRerankableDocument implements ShardedDocument {

    public long id; // required
    public double rsv; // required
    public String screen_name; // required
    public long epoch; // required
    public String text; // required
    public int followers_count; // required
    public int statuses_count; // required
    public String lang; // required
    public long in_reply_to_status_id; // required
    public long in_reply_to_user_id; // required
    public long retweeted_status_id; // required
    public long retweeted_user_id; // required
    public int retweeted_count; // required

    public StatusDocument(StatusDocument other) {
        this.id = other.id;
        this.rsv = other.rsv;
        this.screen_name = other.screen_name;
        this.epoch = other.epoch;
        this.text = other.text;
        this.followers_count = other.followers_count;
        this.statuses_count = other.statuses_count;
        this.lang = other.lang;
        this.in_reply_to_status_id = other.in_reply_to_status_id;
        this.in_reply_to_user_id = other.in_reply_to_user_id;
        this.retweeted_status_id = other.retweeted_status_id;
        this.retweeted_user_id = other.retweeted_user_id;
        this.retweeted_count = other.retweeted_count;
        this.setDocVector(other.getDocVector());
        this.setFeatures(other.getFeatures());
    }

    public StatusDocument(TResult other) {
        this.id = other.id;
        this.rsv = other.rsv;
        this.screen_name = other.screen_name;
        this.epoch = other.epoch;
        this.text = other.text;
        this.followers_count = other.followers_count;
        this.statuses_count = other.statuses_count;
        this.lang = other.lang;
        this.in_reply_to_status_id = other.in_reply_to_status_id;
        this.in_reply_to_user_id = other.in_reply_to_user_id;
        this.retweeted_status_id = other.retweeted_status_id;
        this.retweeted_user_id = other.retweeted_user_id;
        this.retweeted_count = other.retweeted_count;
    }

    public StatusDocument(org.apache.lucene.document.Document hit) {
        if (hit.get(IndexStatuses.StatusField.SCREEN_NAME.name) != null) {
            this.id = (Long) hit.getField(IndexStatuses.StatusField.ID.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.SCREEN_NAME.name) != null) {
            this.screen_name = hit.get(IndexStatuses.StatusField.SCREEN_NAME.name);
        } else {
            this.screen_name = hit.get(WikipediaManager.TITLE_FIELD);
        }

        if (hit.get(IndexStatuses.StatusField.EPOCH.name) != null) {
            this.epoch = (Long) hit.getField(IndexStatuses.StatusField.EPOCH.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.TEXT.name) != null) {
            this.text = hit.get(IndexStatuses.StatusField.TEXT.name);
        } else {
            this.text = hit.get(WikipediaManager.TEXT_FIELD);
        }

        if (hit.get(IndexStatuses.StatusField.FOLLOWERS_COUNT.name) != null) {
            this.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
        }

        if (hit.get(IndexStatuses.StatusField.STATUSES_COUNT.name) != null) {
            this.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();
        }

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

    @Override
    public String getId() {
        return Long.toString(id);
    }

    public void setId(String id) {
        this.id = Long.parseLong(id);
    }

    @Override
    public double getRsv() {
        return rsv;
    }

    @Override
    public void setRsv(double rsv) {
        this.rsv = rsv;
    }

    public String getScreen_name() {
        return screen_name;
    }

    public void setScreen_name(String screen_name) {
        this.screen_name = screen_name;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public void setText(String text) {
        this.text = text;
    }

    public int getFollowers_count() {
        return followers_count;
    }

    public void setFollowers_count(int followers_count) {
        this.followers_count = followers_count;
    }

    public int getStatuses_count() {
        return statuses_count;
    }

    public void setStatuses_count(int statuses_count) {
        this.statuses_count = statuses_count;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public long getIn_reply_to_status_id() {
        return in_reply_to_status_id;
    }

    public void setIn_reply_to_status_id(long in_reply_to_status_id) {
        this.in_reply_to_status_id = in_reply_to_status_id;
    }

    public long getIn_reply_to_user_id() {
        return in_reply_to_user_id;
    }

    public void setIn_reply_to_user_id(long in_reply_to_user_id) {
        this.in_reply_to_user_id = in_reply_to_user_id;
    }

    public long getRetweeted_status_id() {
        return retweeted_status_id;
    }

    public void setRetweeted_status_id(long retweeted_status_id) {
        this.retweeted_status_id = retweeted_status_id;
    }

    public long getRetweeted_user_id() {
        return retweeted_user_id;
    }

    public void setRetweeted_user_id(long retweeted_user_id) {
        this.retweeted_user_id = retweeted_user_id;
    }

    public int getRetweeted_count() {
        return retweeted_count;
    }

    public void setRetweeted_count(int retweeted_count) {
        this.retweeted_count = retweeted_count;
    }

    @Override
    public String getShardId() {
        return getScreen_name();
    }

    @Override
    public void setShardId(String shardId) {
        setScreen_name(shardId);
    }
}
