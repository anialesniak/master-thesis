package com.lakesidemutual.policyexpirationconstraints;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

@JsonSubTypes({@JsonSubTypes.Type(InsuranceQuoteExpiredEvent.class), @JsonSubTypes.Type(PolicyCreatedEvent.class)})
public class InsuranceQuoteEvent implements DomainEvent {
    private Long insuranceQuoteRequestId;

    public InsuranceQuoteEvent() {
    }

    public InsuranceQuoteEvent(Long insuranceQuoteRequestId) {
        this.insuranceQuoteRequestId = insuranceQuoteRequestId;
    }

    public Long getInsuranceQuoteRequestId() {
        return insuranceQuoteRequestId;
    }

    public void setInsuranceQuoteRequestId(Long insuranceQuoteRequestId) {
        this.insuranceQuoteRequestId = insuranceQuoteRequestId;
    }
}
