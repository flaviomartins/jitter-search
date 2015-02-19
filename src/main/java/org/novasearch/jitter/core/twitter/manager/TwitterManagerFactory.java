package org.novasearch.jitter.core.twitter.manager;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class TwitterManagerFactory {

    @NotEmpty
    private String database;

    @NotEmpty
    private String collection;

    @NotEmpty
    private List<String> users;

    @JsonProperty
    public String getDatabase() {
        return database;
    }

    @JsonProperty
    public void setDatabase(String database) {
        this.database = database;
    }

    @JsonProperty("collection")
    public String getCollection() {
        return collection;
    }

    @JsonProperty("collection")
    public void setCollection(String collection) {
        this.collection = collection;
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
        final TwitterManager twitterManager = new TwitterManager(database, collection, users);
        environment.lifecycle().manage(twitterManager);
        return twitterManager;
    }

}
