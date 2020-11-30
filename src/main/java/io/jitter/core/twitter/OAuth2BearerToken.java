package io.jitter.core.twitter;

import com.google.common.base.Preconditions;

public class OAuth2BearerToken {

    private final String bearerToken;

    public OAuth2BearerToken(String bearerToken) {
        this.bearerToken = Preconditions.checkNotNull(bearerToken);
    }

    public String getBearerToken() {
        return bearerToken;
    }

}
