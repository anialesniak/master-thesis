package com.lakesidemutual.customerselfservice.domain.insurancequoterequest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

@JsonIgnoreProperties(value = { "$type" })
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
