package org.novasearch.jitter.core.twitter;

import com.google.common.base.Preconditions;

public class OAuth1 {

    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String tokenSecret;

    public OAuth1(String consumerKey, String consumerSecret, String token, String tokenSecret) {
        this.consumerKey = Preconditions.checkNotNull(consumerKey);
        this.consumerSecret = Preconditions.checkNotNull(consumerSecret);

        this.token = Preconditions.checkNotNull(token);
        this.tokenSecret = Preconditions.checkNotNull(tokenSecret);
    }

    public String getConsumerKey() {
        return consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public String getToken() {
        return token;
    }

    public String getTokenSecret() {
        return tokenSecret;
    }
}
