package com.lakesidemutual.customerselfservice.domain.insurancequoterequest;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "$type")
@JsonSubTypes({@JsonSubTypes.Type(value = InsuranceQuoteExpiredEvent.class, name = "InsuranceQuoteExpiredEvent"),
        @JsonSubTypes.Type(value = PolicyCreatedEvent.class, name = "PolicyCreatedEvent")})
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
