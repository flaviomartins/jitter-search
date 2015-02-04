package org.novasearch.jitter.rs;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;

public class ResourceSelectionFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String method;

    @NotEmpty
    private String twitterMode;

    @Valid
    @NotNull
    private boolean removeDuplicates;

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty
    public String getMethod() {
        return method;
    }

    @JsonProperty
    public void setMethod(String method) {
        this.method = method;
    }

    @JsonProperty("twitter")
    public String getTwitterMode() {
        return twitterMode;
    }

    @JsonProperty("twitter")
    public void setTwitterMode(String twitterMode) {
        this.twitterMode = twitterMode;
    }

    @JsonProperty
    public boolean isRemoveDuplicates() {
        return removeDuplicates;
    }

    @JsonProperty
    public void setRemoveDuplicates(boolean removeDuplicates) {
        this.removeDuplicates = removeDuplicates;
    }

    public ResourceSelection build(Environment environment) throws IOException {
        final ResourceSelection resourceSelection = new ResourceSelection(index, method, twitterMode, removeDuplicates);
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() {
            }

            @Override
            public void stop() throws IOException {
                resourceSelection.close();
            }
        });
        return resourceSelection;
    }

}
