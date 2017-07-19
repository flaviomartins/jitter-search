package io.jitter.core.wikipedia;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.io.IOException;

public class WikipediaManagerFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String stopwords;

    @Min(1)
    @Max(5000)
    private float mu;

    @NotEmpty
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

    @JsonProperty
    public String getCat2topic() {
        return cat2topic;
    }

    @JsonProperty
    public void setCat2topic(String cat2topic) {
        this.cat2topic = cat2topic;
    }

    public WikipediaManager build(Environment environment) throws IOException {
        final WikipediaManager wikipediaManager = new WikipediaManager(index, false, stopwords, mu, cat2topic);
        environment.lifecycle().manage(wikipediaManager);
        return wikipediaManager;
    }
}
