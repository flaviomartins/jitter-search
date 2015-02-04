package org.novasearch.jitter.core.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;

public class SearchManagerFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String database;

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty
    public String getDatabase() {
        return database;
    }

    @JsonProperty
    public void setDatabase(String database) {
        this.database = database;
    }

    public SearchManager build(Environment environment) throws IOException {
        final SearchManager searchManager = new SearchManager(index, database);
        environment.lifecycle().manage(searchManager);
        return searchManager;
    }
}
