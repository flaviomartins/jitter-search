package org.novasearch.jitter.core.twitter.archiver;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class TwitterArchiverFactory {

    @NotEmpty
    private String collectionPath;

    @NotEmpty
    private List<String> users;

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

    public TwitterArchiver build(Environment environment) {
        final TwitterArchiver twitterArchiver = new TwitterArchiver(collectionPath, users);
        environment.lifecycle().manage(twitterArchiver);
        return twitterArchiver;
    }

}
