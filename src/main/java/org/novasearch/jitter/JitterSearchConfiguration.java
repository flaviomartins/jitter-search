package org.novasearch.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

public class JitterSearchConfiguration extends Configuration {

    @NotEmpty
    private String index;

    @NotEmpty
    private String reputationFile;

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

}
