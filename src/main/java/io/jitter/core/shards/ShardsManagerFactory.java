package io.jitter.core.shards;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

public class ShardsManagerFactory {

    private String collection;

    @NotEmpty
    private String index;

    @NotEmpty
    private String stopwords;

    @Min(1)
    @Max(5000)
    private float mu;

    @NotEmpty
    private String method;

    @Valid
    @NotNull
    private boolean removeDuplicates;

    private Map<String, Set<String>> topics;

    @JsonProperty
    public String getCollection() {
        return collection;
    }

    @JsonProperty
    public void setCollection(String collection) {
        this.collection = collection;
    }

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty
    public String getStopwords() {
        return stopwords;
    }

    @JsonProperty
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
    public String getMethod() {
        return method;
    }

    @JsonProperty
    public void setMethod(String method) {
        this.method = method;
    }

    @JsonProperty
    public boolean isRemoveDuplicates() {
        return removeDuplicates;
    }

    @JsonProperty
    public void setRemoveDuplicates(boolean removeDuplicates) {
        this.removeDuplicates = removeDuplicates;
    }

    @JsonProperty
    public Map<String, Set<String>> getTopics() {
        return topics;
    }

    @JsonProperty
    public void setTopics(Map<String, Set<String>> topics) {
        this.topics = topics;
    }

    public ShardsManager build(Environment environment, boolean live) {
        final ShardsManager shardsManager = new ShardsManager(collection, index, stopwords, mu, method, removeDuplicates, live, topics);
        environment.lifecycle().manage(shardsManager);
        return shardsManager;
    }

}
