package org.novasearch.jitter.twitter_archiver;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class TwitterArchiverFactory {

    @NotEmpty
    private String directory;

    @NotEmpty
    private List<String> users;

    @JsonProperty
    public String getDirectory() {
        return directory;
    }

    @JsonProperty
    public void setDirectory(String directory) {
        this.directory = directory;
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
        final TwitterArchiver twitterArchiver = new TwitterArchiver(directory, users);
        environment.lifecycle().manage(twitterArchiver);
        return twitterArchiver;
    }

}
