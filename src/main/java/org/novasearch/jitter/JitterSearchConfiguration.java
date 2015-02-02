package org.novasearch.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;
import org.novasearch.jitter.rs.ResourceSelectionFactory;
import org.novasearch.jitter.twitter.TwitterManagerFactory;
import org.novasearch.jitter.twitter_archiver.TwitterArchiverFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class JitterSearchConfiguration extends Configuration {

    @NotEmpty
    private String index;

    @Valid
    @NotNull
    private TwitterManagerFactory twitterManagerFactory = new TwitterManagerFactory();

    @Valid
    @NotNull
    private TwitterArchiverFactory twitterArchiverFactory = new TwitterArchiverFactory();

    @Valid
    @NotNull
    private ResourceSelectionFactory resourceSelectionFactory = new ResourceSelectionFactory();

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
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

    @JsonProperty("twitter_archiver")
    public TwitterArchiverFactory getTwitterArchiverFactory() {
        return twitterArchiverFactory;
    }

    @JsonProperty("twitter_archiver")
    public void setTwitterArchiverFactory(TwitterArchiverFactory twitterArchiverFactory) {
        this.twitterArchiverFactory = twitterArchiverFactory;
    }
}
