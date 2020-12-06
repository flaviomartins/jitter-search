package io.jitter.core.twitter.manager;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import io.jitter.core.twitter.OAuth1Factory;
import io.jitter.core.twitter.OAuth2BearerTokenFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Set;

public class TwitterManagerFactory {

    @Valid
    @NotNull
    private OAuth1Factory oAuth1Factory = new OAuth1Factory();

    @Valid
    @NotNull
    private OAuth2BearerTokenFactory oAuth2BearerTokenFactory = new OAuth2BearerTokenFactory();

    @NotEmpty
    private String databasePath;

    @NotEmpty
    private String collectionPath;

    @JsonProperty("oauth")
    public OAuth1Factory getOAuth1Factory() {
        return oAuth1Factory;
    }

    @JsonProperty("oauth")
    public void setOAuth1Factory(OAuth1Factory oAuth1Factory) {
        this.oAuth1Factory = oAuth1Factory;
    }

    @JsonProperty("oauth2")
    public OAuth2BearerTokenFactory getOAuth2BearerTokenFactory() {
        return oAuth2BearerTokenFactory;
    }

    @JsonProperty("oauth2")
    public void setOAuth2BearerTokenFactory(OAuth2BearerTokenFactory oAuth2BearerTokenFactory) {
        this.oAuth2BearerTokenFactory = oAuth2BearerTokenFactory;
    }

    @JsonProperty("database")
    public String getDatabasePath() {
        return databasePath;
    }

    @JsonProperty("database")
    public void setDatabasePath(String databasePath) {
        this.databasePath = databasePath;
    }

    @JsonProperty("collection")
    public String getCollectionPath() {
        return collectionPath;
    }

    @JsonProperty("collection")
    public void setCollectionPath(String collectionPath) {
        this.collectionPath = collectionPath;
    }

    public TwitterManager build(Environment environment) {
        final TwitterManager twitterManager = new TwitterManager();
        environment.lifecycle().manage(twitterManager);
        return twitterManager;
    }

}
