package io.jitter.core.wikipedia;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.setup.Environment;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.io.IOException;

public class WikipediaManagerFactory {

    @NotBlank
    private String index;

    @NotBlank
    private String stopwords;

    @Min(1)
    @Max(5000)
    private float mu;

    @NotBlank
    private String cat2topic;

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

    public WikipediaManager build(Environment environment) throws IOException {
        final WikipediaManager wikipediaManager = new WikipediaManager(index, false, stopwords, mu);
        environment.lifecycle().manage(wikipediaManager);
        return wikipediaManager;
    }
}
