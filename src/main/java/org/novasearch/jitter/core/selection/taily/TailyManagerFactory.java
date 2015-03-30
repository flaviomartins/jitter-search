package org.novasearch.jitter.core.selection.taily;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TailyManagerFactory {

    @NotEmpty
    private String index;

    @NotNull
    private int mu;

    @NotNull
    private int nc;

    @NotEmpty
    private List<String> users;

    private Map<String, List<String>> topics;

    @JsonProperty("index")
    public String getIndex() {
        return index;
    }

    @JsonProperty("index")
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty
    public int getMu() {
        return mu;
    }

    @JsonProperty
    public void setMu(int mu) {
        this.mu = mu;
    }

    @JsonProperty
    public int getNc() {
        return nc;
    }

    @JsonProperty
    public void setNc(int nc) {
        this.nc = nc;
    }

    @JsonProperty
    public List<String> getUsers() {
        return users;
    }

    @JsonProperty
    public void setUsers(List<String> users) {
        this.users = users;
    }

    @JsonProperty
    public Map<String, List<String>> getTopics() {
        return topics;
    }

    @JsonProperty
    public void setTopics(Map<String, List<String>> topics) {
        this.topics = topics;
    }

    public TailyManager build(Environment environment) throws IOException {
        final TailyManager tailyManager = new TailyManager(index, mu, nc, users, topics);
        environment.lifecycle().manage(tailyManager);
        return tailyManager;
    }

}
