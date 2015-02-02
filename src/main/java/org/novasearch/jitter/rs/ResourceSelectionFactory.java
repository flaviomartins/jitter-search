package org.novasearch.jitter.rs;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;
import org.novasearch.jitter.twitter.TwitterManager;

import java.io.IOException;

public class ResourceSelectionFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String twitterMode;

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty("twitter")
    public String getTwitterMode() {
        return twitterMode;
    }

    @JsonProperty("twitter")
    public void setTwitterMode(String twitterMode) {
        this.twitterMode = twitterMode;
    }

    public ResourceSelection build(Environment environment) throws IOException {
        final ResourceSelection resourceSelection = new ResourceSelection(index, twitterMode);
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() {
            }

            @Override
            public void stop() {
                resourceSelection.close();
            }
        });
        return resourceSelection;
    }

}
