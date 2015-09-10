package io.jitter.core.selection;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

public class SelectionManagerFactory {

    @NotEmpty
    private String index;

    @NotEmpty
    private String method;

    @Valid
    @NotNull
    private boolean removeDuplicates;

    private Map<String, Set<String>> topics;

    @JsonProperty("index")
    public String getIndex() {
        return index;
    }

    @JsonProperty("index")
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

    @JsonProperty
    public boolean isRemoveDuplicates() {
        return removeDuplicates;
    }

    @JsonProperty
    public void setRemoveDuplicates(boolean removeDuplicates) {
        this.removeDuplicates = removeDuplicates;
    }

    @JsonProperty
    public Map<String, Set<String>> getTopics() {
        return topics;
    }

    @JsonProperty
    public void setTopics(Map<String, Set<String>> topics) {
        this.topics = topics;
    }

    public SelectionManager build(Environment environment) {
        final SelectionManager selectionManager = new SelectionManager(index, method, removeDuplicates, topics);
        environment.lifecycle().manage(selectionManager);
        return selectionManager;
    }

}