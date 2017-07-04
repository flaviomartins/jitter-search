package io.jitter.core.wikipedia;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class WikipediaManagerFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String stopwords;

    @Min(1)
    @Max(5000)
    private float mu;

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

    public WikipediaManager build(Environment environment) {
        final WikipediaManager wikipediaManager = new WikipediaManager(index, false, stopwords, mu);
        environment.lifecycle().manage(wikipediaManager);
        return wikipediaManager;
    }
}
