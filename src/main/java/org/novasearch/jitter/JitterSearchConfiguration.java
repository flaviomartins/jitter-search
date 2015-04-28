package org.novasearch.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.novasearch.jitter.core.search.SearchManagerFactory;
import org.novasearch.jitter.core.selection.SelectionManagerFactory;
import org.novasearch.jitter.core.selection.taily.TailyManagerFactory;
import org.novasearch.jitter.core.twitter.manager.TwitterManagerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class JitterSearchConfiguration extends Configuration {

    @Valid
    @NotNull
    private boolean live;

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
    private TailyManagerFactory tailyManagerFactory = new TailyManagerFactory();

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

    @JsonProperty("taily")
    public TailyManagerFactory getTailyManagerFactory() {
        return tailyManagerFactory;
    }

    @JsonProperty("taily")
    public void setTailyManagerFactory(TailyManagerFactory tailyManagerFactory) {
        this.tailyManagerFactory = tailyManagerFactory;
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
}
