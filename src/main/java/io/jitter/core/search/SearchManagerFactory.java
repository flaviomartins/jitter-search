package io.jitter.core.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.setup.Environment;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class SearchManagerFactory {

    @NotBlank
    private String index;

    @NotBlank
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

    public SearchManager build(Environment environment, boolean live) {
        final SearchManager searchManager = new SearchManager(index, stopwords, mu, live);
        environment.lifecycle().manage(searchManager);
        return searchManager;
    }
}
