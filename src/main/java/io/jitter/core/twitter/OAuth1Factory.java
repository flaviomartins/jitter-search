package io.jitter.core.twitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;public class OAuth1Factory {

    @NotBlank
    private String consumerKey;

    @NotBlank
    private String consumerSecret;

    @NotBlank
    private String token;

    @NotBlank
    private String tokenSecret;

    @JsonProperty
    public String getConsumerKey() {
        return consumerKey;
    }

    @JsonProperty
    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    @JsonProperty
    public String getConsumerSecret() {
        return consumerSecret;
    }

    @JsonProperty
    public void setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
    }

    @JsonProperty
    public String getToken() {
        return token;
    }

    @JsonProperty
    public void setToken(String token) {
        this.token = token;
    }

    @JsonProperty
    public String getTokenSecret() {
        return tokenSecret;
    }

    @JsonProperty
    public void setTokenSecret(String tokenSecret) {
        this.tokenSecret = tokenSecret;
    }

    public OAuth1 build() {
        return new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
    }

}
