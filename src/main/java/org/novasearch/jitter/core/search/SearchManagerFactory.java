package org.novasearch.jitter.core.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;

public class SearchManagerFactory {

    @NotEmpty
    private String indexPath;

    @NotEmpty
    private String databasePath;

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

    public SearchManager build(Environment environment) throws IOException {
        final SearchManager searchManager = new SearchManager(indexPath, databasePath);
        environment.lifecycle().manage(searchManager);
        return searchManager;
    }
}
