package org.novasearch.jitter.rs;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.List;

public class ResourceSelectionFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private List<String> twitter;

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty
    public List<String> getTwitter() {
        return twitter;
    }

    @JsonProperty
    public void setTwitter(List<String> twitter) {
        this.twitter = twitter;
    }

    public ResourceSelection build(Environment environment) throws IOException {
        final ResourceSelection rs = new ResourceSelection(index, twitter);
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
