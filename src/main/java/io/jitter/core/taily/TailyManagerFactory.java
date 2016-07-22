package io.jitter.core.taily;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class TailyManagerFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String dbPath;

    @NotEmpty
    private String stopwords;

    @Min(1)
    @Max(5000)
    private float mu;

    @Min(1)
    private float nc;

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

    @JsonProperty("db")
    public String getDbPath() {
        return dbPath;
    }

    @JsonProperty("db")
    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    @JsonProperty("stopwords")
    public String getStopwords() {
        return stopwords;
    }

    @JsonProperty("stopwords")
    public void setStopwords(String stopwords) {
        this.stopwords = stopwords;
    }

    @JsonProperty
    public float getMu() {
        return mu;
    }

    @JsonProperty
    public void setMu(float mu) {
        this.mu = mu;
    }

    @JsonProperty
    public float getNc() {
        return nc;
    }

    @JsonProperty
    public void setNc(float nc) {
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

    public TailyManager build(Environment environment) {
        final TailyManager tailyManager = new TailyManager(dbPath, index, stopwords, mu, nc, users, topics);
        environment.lifecycle().manage(tailyManager);
        return tailyManager;
    }

}
