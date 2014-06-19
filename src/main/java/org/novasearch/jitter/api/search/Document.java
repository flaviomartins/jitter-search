package org.novasearch.jitter.api.search;

import cc.twittertools.thrift.gen.TResult;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Document /* cannot extend TResult */ {
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
    public String[] entities;
    public Map<String, Double> reputation;

    public Document() {
        // Jackson deserialization
    }

    public Document(Document result) {
        id = result.id;
        rsv = result.rsv;
        screen_name = result.screen_name;
        epoch = result.epoch;
        text = result.text;
        followers_count = result.followers_count;
        statuses_count = result.statuses_count;
        lang = result.lang;
        in_reply_to_status_id = result.in_reply_to_status_id;
        in_reply_to_user_id = result.in_reply_to_user_id;
        retweeted_status_id = result.retweeted_status_id;
        retweeted_user_id = result.retweeted_user_id;
        retweeted_count = result.retweeted_count;
        entities = result.entities;
        reputation = result.reputation;
    }

    @JsonProperty
    public long getId() {
        return id;
    }

    @JsonProperty
    public double getRsv() {
        return rsv;
    }

    @JsonProperty
    public String getScreen_name() {
        return screen_name;
    }

    @JsonProperty
    public long getEpoch() {
        return epoch;
    }

    @JsonProperty
    public String getText() {
        return text;
    }

    @JsonProperty
    public int getFollowers_count() {
        return followers_count;
    }

    @JsonProperty
    public int getStatuses_count() {
        return statuses_count;
    }

    @JsonProperty
    public String getLang() {
        return lang;
    }

    @JsonProperty
    public long getIn_reply_to_status_id() {
        return in_reply_to_status_id;
    }

    @JsonProperty
    public long getIn_reply_to_user_id() {
        return in_reply_to_user_id;
    }

    @JsonProperty
    public long getRetweeted_status_id() {
        return retweeted_status_id;
    }

    @JsonProperty
    public long getRetweeted_user_id() {
        return retweeted_user_id;
    }

    @JsonProperty
    public int getRetweeted_count() {
        return retweeted_count;
    }

    @JsonProperty
    public String[] getEntities() {
        return entities;
    }

    @JsonProperty
    public Map<String, Double> getReputation() {
        return reputation;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setRsv(double rsv) {
        this.rsv = rsv;
    }

    public void setScreen_name(String screen_name) {
        this.screen_name = screen_name;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setFollowers_count(int followers_count) {
        this.followers_count = followers_count;
    }

    public void setStatuses_count(int statuses_count) {
        this.statuses_count = statuses_count;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public void setIn_reply_to_status_id(long in_reply_to_status_id) {
        this.in_reply_to_status_id = in_reply_to_status_id;
    }

    public void setIn_reply_to_user_id(long in_reply_to_user_id) {
        this.in_reply_to_user_id = in_reply_to_user_id;
    }

    public void setRetweeted_status_id(long retweeted_status_id) {
        this.retweeted_status_id = retweeted_status_id;
    }

    public void setRetweeted_user_id(long retweeted_user_id) {
        this.retweeted_user_id = retweeted_user_id;
    }

    public void setRetweeted_count(int retweeted_count) {
        this.retweeted_count = retweeted_count;
    }

    public void setEntities(String[] entities) {
        this.entities = entities;
    }

    public void setReputation(Map<String, Double> reputation) {
        this.reputation = reputation;
    }
}
