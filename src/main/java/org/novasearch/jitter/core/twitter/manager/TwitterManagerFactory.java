package org.novasearch.jitter.core.twitter.manager;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;
import org.novasearch.jitter.core.twitter.OAuth1Factory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class TwitterManagerFactory {

    @Valid
    @NotNull
    private OAuth1Factory oAuth1Factory = new OAuth1Factory();

    @NotEmpty
    private String databasePath;

    @NotEmpty
    private String collectionPath;

    @NotEmpty
    private List<String> users;

    @JsonProperty("oauth")
    public OAuth1Factory getoAuth1Factory() {
        return oAuth1Factory;
    }

    @JsonProperty("oauth")
    public void setoAuth1Factory(OAuth1Factory oAuth1Factory) {
        this.oAuth1Factory = oAuth1Factory;
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

    @JsonProperty
    public List<String> getUsers() {
        return users;
    }

    @JsonProperty
    public void setUsers(List<String> users) {
        this.users = users;
    }

    public TwitterManager build(Environment environment) {
        final TwitterManager twitterManager = new TwitterManager(databasePath, collectionPath, users);
        environment.lifecycle().manage(twitterManager);
        return twitterManager;
    }

}
