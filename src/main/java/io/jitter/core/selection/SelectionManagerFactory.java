package io.jitter.core.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.Map;
import java.util.Set;

public class SelectionManagerFactory {

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
    public Map<String, Set<String>> getTopics() {
        return topics;
    }

    @JsonProperty
    public void setTopics(Map<String, Set<String>> topics) {
        this.topics = topics;
    }

    public SelectionManager build(Environment environment, boolean live) {
        final SelectionManager selectionManager = new SelectionManager(collection, index, stopwords, mu, method, live, topics);
        environment.lifecycle().manage(selectionManager);
        return selectionManager;
    }

}
