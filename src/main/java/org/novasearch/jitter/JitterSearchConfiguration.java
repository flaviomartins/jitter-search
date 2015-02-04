package org.novasearch.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.novasearch.jitter.core.search.SearchManagerFactory;
import org.novasearch.jitter.rs.ResourceSelectionFactory;
import org.novasearch.jitter.twitter.TwitterManagerFactory;
import org.novasearch.jitter.twitter_archiver.TwitterArchiverFactory;

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
    private ResourceSelectionFactory resourceSelectionFactory = new ResourceSelectionFactory();

    @JsonProperty("search")
    public SearchManagerFactory getSearchManagerFactory() {
        return searchManagerFactory;
    }

    @JsonProperty("search")
    public void setSearchManagerFactory(SearchManagerFactory searchManagerFactory) {
        this.searchManagerFactory = searchManagerFactory;
    }

    @JsonProperty("rs")
    public ResourceSelectionFactory getResourceSelectionFactory() {
        return resourceSelectionFactory;
    }

    @JsonProperty("rs")
    public void setResourceSelectionFactory(ResourceSelectionFactory resourceSelectionFactory) {
        this.resourceSelectionFactory = resourceSelectionFactory;
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
