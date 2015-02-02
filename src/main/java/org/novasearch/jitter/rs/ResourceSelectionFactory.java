package org.novasearch.jitter.rs;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;
import org.novasearch.jitter.twitter.TwitterManager;

import java.io.IOException;
import java.util.List;

public class ResourceSelectionFactory {

    @NotEmpty
    private String index;

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    public ResourceSelection build(Environment environment, TwitterManager twitterManager) throws IOException {
        final ResourceSelection rs = new ResourceSelection(index, twitterManager);
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() {
            }

            @Override
            public void stop() {
                rs.close();
            }
        });
        return rs;
    }

}
