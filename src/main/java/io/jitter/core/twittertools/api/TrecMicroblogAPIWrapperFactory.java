package io.jitter.core.twittertools.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

public class TrecMicroblogAPIWrapperFactory {

    @NotEmpty
    private String host;

    private int port;

    @NotEmpty
    private String group;

    @NotEmpty
    private String token;

    @NotEmpty
    private String cacheDir;

    private boolean useCache;

    @NotEmpty
    private String collectDb;

    @NotEmpty
    private String stopwords;

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public String getGroup() {
        return group;
    }

    @JsonProperty
    public String getToken() {
        return token;
    }

    @JsonProperty
    public String getCacheDir() {
        return cacheDir;
    }

    @JsonProperty
    public boolean isUseCache() {
        return useCache;
    }

    @JsonProperty
    public String getCollectDb() {
        return collectDb;
    }

    @JsonProperty
    public String getStopwords() {
        return stopwords;
    }

    @JsonProperty
    public void setStopwords(String stopwords) {
        this.stopwords = stopwords;
    }

    public TrecMicroblogAPIWrapper build(Environment environment) {
        final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper = new TrecMicroblogAPIWrapper(host, port, group, token, cacheDir, useCache, collectDb, stopwords);
        environment.lifecycle().manage(trecMicroblogAPIWrapper);
        return trecMicroblogAPIWrapper;
    }
}