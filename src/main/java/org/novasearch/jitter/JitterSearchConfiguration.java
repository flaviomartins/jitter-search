package org.novasearch.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.novasearch.jitter.core.search.SearchManagerFactory;
import org.novasearch.jitter.core.selection.SelectionManagerFactory;
import org.novasearch.jitter.core.twitter.TwitterManagerFactory;
import org.novasearch.jitter.core.twitter_archiver.TwitterArchiverFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class JitterSearchConfiguration extends Configuration {

    @Valid
    @NotNull
    private SearchManagerFactory searchManagerFactory = new SearchManagerFactory();

    @Valid
    @NotNull
    private TwitterManagerFactory twitterManagerFactory = new TwitterManagerFactory();

    @Valid
    @NotNull
    private TwitterArchiverFactory twitterArchiverFactory = new TwitterArchiverFactory();

    @Valid
    @NotNull
    private SelectionManagerFactory selectionManagerFactory = new SelectionManagerFactory();

    @JsonProperty("search")
    public SearchManagerFactory getSearchManagerFactory() {
        return searchManagerFactory;
    }

    @JsonProperty("search")
    public void setSearchManagerFactory(SearchManagerFactory searchManagerFactory) {
        this.searchManagerFactory = searchManagerFactory;
    }

    @JsonProperty("selection")
    public SelectionManagerFactory getSelectionManagerFactory() {
        return selectionManagerFactory;
    }

    @JsonProperty("selection")
    public void setSelectionManagerFactory(SelectionManagerFactory selectionManagerFactory) {
        this.selectionManagerFactory = selectionManagerFactory;
    }

    @JsonProperty("twitter")
    public TwitterManagerFactory getTwitterManagerFactory() {
        return twitterManagerFactory;
    }

    @JsonProperty("twitter")
    public void setTwitterManagerFactory(TwitterManagerFactory twitterManagerFactory) {
        this.twitterManagerFactory = twitterManagerFactory;
    }

    @JsonProperty("twitterArchiver")
    public TwitterArchiverFactory getTwitterArchiverFactory() {
        return twitterArchiverFactory;
    }

    @JsonProperty("twitterArchiver")
    public void setTwitterArchiverFactory(TwitterArchiverFactory twitterArchiverFactory) {
        this.twitterArchiverFactory = twitterArchiverFactory;
    }
}
