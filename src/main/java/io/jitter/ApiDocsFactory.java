package io.jitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.jaxrs.config.BeanConfig;

/**
 * Created by fmartins on 04/11/16.
 */
public class ApiDocsFactory {
    private String title;
    private String description;
    private String version;
    private String[] schemes;
    private String host;
    private String basePath;

    @JsonProperty
    public String getTitle() {
        return title;
    }

    @JsonProperty
    public void setTitle(String title) {
        this.title = title;
    }

    @JsonProperty
    public String getDescription() {
        return description;
    }

    @JsonProperty
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty
    public String getVersion() {
        return version;
    }

    @JsonProperty
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonProperty
    public String[] getSchemes() {
        return schemes;
    }

    @JsonProperty
    public void setSchemes(String[] schemes) {
        this.schemes = schemes;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public void setHost(String host) {
        this.host = host;
    }

    @JsonProperty("base_path")
    public String getBasePath() {
        return basePath;
    }

    @JsonProperty("base_path")
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public BeanConfig build() {
        final BeanConfig beanConfig = new BeanConfig();
        beanConfig.setTitle(title);
        beanConfig.setDescription(description);
        beanConfig.setVersion(version);
        beanConfig.setSchemes(schemes);
        beanConfig.setHost(host);
        beanConfig.setBasePath(basePath);
        return beanConfig;
    }
}
