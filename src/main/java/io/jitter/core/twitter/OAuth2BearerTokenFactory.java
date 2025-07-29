package io.jitter.core.twitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class OAuth2BearerTokenFactory {

    @NotBlank
    private String bearerToken;

    @JsonProperty
    public String getBearerToken() {
        return bearerToken;
    }

    @JsonProperty
    public void setBearerToken(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    public OAuth2BearerToken build() {
        return new OAuth2BearerToken(bearerToken);
    }

}
