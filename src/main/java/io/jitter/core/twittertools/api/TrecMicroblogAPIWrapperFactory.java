package io.jitter.core.twittertools.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.setup.Environment;
import jakarta.validation.constraints.NotBlank;public class TrecMicroblogAPIWrapperFactory {

    @NotBlank
    private String host;

    private int port;

    @NotBlank
    private String group;

    @NotBlank
    private String token;

    @NotBlank
    private String cacheDir;

    private boolean useCache;

    @NotBlank
    private String stopwords;

    private String stats;

    private String statsDb;

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
    public String getStopwords() {
        return stopwords;
    }

    @JsonProperty
    public String getStats() {
        return stats;
    }

    @JsonProperty
    public String getStatsDb() {
        return statsDb;
    }

    public TrecMicroblogAPIWrapper build(Environment environment) {
        final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper = new TrecMicroblogAPIWrapper(host, port, group, token, cacheDir, useCache, stopwords, stats, statsDb);
        environment.lifecycle().manage(trecMicroblogAPIWrapper);
        return trecMicroblogAPIWrapper;
    }
}
