package com.lakesidemutual.customerselfservice.domain.insurancequoterequest;

import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

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
