package com.lakesidemutual.policyexpirationconstraints;

import java.util.Date;

/**
 * PolicyCreatedEvent is a domain event class that is used to notify the Customer Self-Service Backend
 * when a new Policy has been created after an Insurance Quote has been accepted.
 * */
public class PolicyCreatedEvent extends InsuranceQuoteEvent {
	private Date date;
	private String policyId;

	public PolicyCreatedEvent() {
	}

	public PolicyCreatedEvent(Date date, Long insuranceQuoteRequestId, String policyId) {
		super(insuranceQuoteRequestId);
		this.date = date;
		this.policyId = policyId;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getPolicyId() {
		return policyId;
	}

	public void setPolicyId(String policyId) {
		this.policyId = policyId;
	}
}