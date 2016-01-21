package io.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.jitter.core.search.SearchManagerFactory;
import io.jitter.core.selection.taily.TailyManagerFactory;
import io.jitter.core.twitter.manager.TwitterManagerFactory;
import io.jitter.core.selection.SelectionManagerFactory;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapperFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class JitterSearchConfiguration extends Configuration {

    @Valid
    @NotNull
    private boolean live;

    private String statusStreamLogPath;

    private String userStreamLogPath;

    @Valid
    @NotNull
    private boolean cors;

    @Valid
    @NotNull
    private SearchManagerFactory searchManagerFactory = new SearchManagerFactory();

    @Valid
    @NotNull
    private TwitterManagerFactory twitterManagerFactory = new TwitterManagerFactory();

    @Valid
    @NotNull
    private SelectionManagerFactory selectionManagerFactory = new SelectionManagerFactory();

    @Valid
    @NotNull
    private SelectionManagerFactory shardsManagerFactory = new SelectionManagerFactory();

    @Valid
    @NotNull
    private TailyManagerFactory tailyManagerFactory = new TailyManagerFactory();

    @Valid
    @NotNull
    private TrecMicroblogAPIWrapperFactory trecMicroblogAPIWrapperFactory = new TrecMicroblogAPIWrapperFactory();

    @JsonProperty("search")
    public SearchManagerFactory getSearchManagerFactory() {
        return searchManagerFactory;
    }

    @JsonProperty("search")
    public void setSearchManagerFactory(SearchManagerFactory searchManagerFactory) {
        this.searchManagerFactory = searchManagerFactory;
    }

    @JsonProperty("twitter")
    public TwitterManagerFactory getTwitterManagerFactory() {
        return twitterManagerFactory;
    }

    @JsonProperty("twitter")
    public void setTwitterManagerFactory(TwitterManagerFactory twitterManagerFactory) {
        this.twitterManagerFactory = twitterManagerFactory;
    }

    @JsonProperty("selection")
    public SelectionManagerFactory getSelectionManagerFactory() {
        return selectionManagerFactory;
    }

    @JsonProperty("selection")
    public void setSelectionManagerFactory(SelectionManagerFactory selectionManagerFactory) {
        this.selectionManagerFactory = selectionManagerFactory;
    }

    @JsonProperty("shards")
    public SelectionManagerFactory getShardsManagerFactory() {
        return shardsManagerFactory;
    }

    @JsonProperty("shards")
    public void setShardsManagerFactory(SelectionManagerFactory shardsManagerFactory) {
        this.shardsManagerFactory = shardsManagerFactory;
    }

    @JsonProperty("taily")
    public TailyManagerFactory getTailyManagerFactory() {
        return tailyManagerFactory;
    }

    @JsonProperty("taily")
    public void setTailyManagerFactory(TailyManagerFactory tailyManagerFactory) {
        this.tailyManagerFactory = tailyManagerFactory;
    }

    @JsonProperty("twittertools")
    public TrecMicroblogAPIWrapperFactory getTrecMicroblogAPIWrapperFactory() {
        return trecMicroblogAPIWrapperFactory;
    }

    @JsonProperty("twittertools")
    public void setTrecMicroblogAPIWrapperFactory(TrecMicroblogAPIWrapperFactory trecMicroblogAPIWrapperFactory) {
        this.trecMicroblogAPIWrapperFactory = trecMicroblogAPIWrapperFactory;
    }

    @JsonProperty
    public boolean isCors() {
        return cors;
    }

    @JsonProperty
    public void setCors(boolean cors) {
        this.cors = cors;
    }

    @JsonProperty
    public boolean isLive() {
        return live;
    }

    @JsonProperty
    public void setLive(boolean live) {
        this.live = live;
    }

    @JsonProperty("status_log_path")
    public String getStatusStreamLogPath() {
        return statusStreamLogPath;
    }

    @JsonProperty("status_log_path")
    public void setStatusStreamLogPath(String statusStreamLogPath) {
        this.statusStreamLogPath = statusStreamLogPath;
    }

    @JsonProperty("user_log_path")
    public String getUserStreamLogPath() {
        return userStreamLogPath;
    }

    @JsonProperty("user_log_path")
    public void setUserStreamLogPath(String userStreamLogPath) {
        this.userStreamLogPath = userStreamLogPath;
    }
}
