package org.novasearch.jitter.core.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SelectionManagerFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String method;

    @NotEmpty
    private String twitterMode;

    @Valid
    @NotNull
    private boolean removeDuplicates;

    private Map<String, List<String>> topics;

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

    @JsonProperty
    public Map<String, List<String>> getTopics() {
        return topics;
    }

    @JsonProperty
    public void setTopics(Map<String, List<String>> topics) {
        this.topics = topics;
    }

    public SelectionManager build(Environment environment) throws IOException {
        final SelectionManager selectionManager = new SelectionManager(index, method, twitterMode, removeDuplicates, topics);
        environment.lifecycle().manage(selectionManager);
        return selectionManager;
    }

}
