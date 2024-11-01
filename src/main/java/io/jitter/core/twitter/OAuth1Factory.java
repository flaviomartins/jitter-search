package io.jitter.core.twitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class OAuth1Factory {

    @NotEmpty
    private String consumerKey;

    @NotEmpty
    private String consumerSecret;

    @NotEmpty
    private String token;

    @NotEmpty
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
