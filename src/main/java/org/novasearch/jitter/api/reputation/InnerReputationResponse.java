package org.novasearch.jitter.api.reputation;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InnerReputationResponse {

    private double reputation;

    public InnerReputationResponse() {
        // Jackson deserialization
    }

    public InnerReputationResponse(double reputation) {
        this.reputation = reputation;
    }

    @JsonProperty
    public double getReputation() {
        return reputation;
    }

}
