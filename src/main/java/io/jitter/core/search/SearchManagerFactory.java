package io.jitter.core.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

public class SearchManagerFactory {

    @NotEmpty
    private String indexPath;

    @NotEmpty
    private String databasePath;

    @NotEmpty
    private String stopwords;

    @JsonProperty("index")
    public String getIndexPath() {
        return indexPath;
    }

    @JsonProperty("index")
    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    @JsonProperty("database")
    public String getDatabasePath() {
        return databasePath;
    }

    @JsonProperty("database")
    public void setDatabasePath(String databasePath) {
        this.databasePath = databasePath;
    }

    @JsonProperty("stopwords")
    public String getStopwords() {
        return stopwords;
    }

    @JsonProperty("stopwords")
    public void setStopwords(String stopwords) {
        this.stopwords = stopwords;
    }

    public SearchManager build(Environment environment, boolean live) {
        final SearchManager searchManager = new SearchManager(indexPath, databasePath, live, stopwords);
        environment.lifecycle().manage(searchManager);
        return searchManager;
    }
}
