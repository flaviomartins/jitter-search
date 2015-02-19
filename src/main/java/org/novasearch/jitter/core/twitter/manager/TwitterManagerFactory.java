package org.novasearch.jitter.core.twitter.manager;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class TwitterManagerFactory {

    @NotEmpty
    private String databasePath;

    @NotEmpty
    private String collectionPath;

    @NotEmpty
    private List<String> users;

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
