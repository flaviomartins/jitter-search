package org.novasearch.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;
import org.novasearch.jitter.rs.ResourceSelectionFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class JitterSearchConfiguration extends Configuration {

    @NotEmpty
    private String index;

    private String reputationFile;

    @Valid
    @NotNull
    private ResourceSelectionFactory resourceSelectionFactory = new ResourceSelectionFactory();

    @JsonProperty
    public String getIndex() {
        return index;
    }

    @JsonProperty
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty
    public String getReputationFile() {
        return reputationFile;
    }

    @JsonProperty
    public void setReputationFile(String reputationFile) {
        this.reputationFile = reputationFile;
    }

    @JsonProperty("rs")
    public ResourceSelectionFactory getResourceSelectionFactory() {
        return resourceSelectionFactory;
    }

    @JsonProperty("rs")
    public void setResourceSelectionFactory(ResourceSelectionFactory resourceSelectionFactory) {
        this.resourceSelectionFactory = resourceSelectionFactory;
    }

}
