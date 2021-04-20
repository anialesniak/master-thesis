package com.lakesidemutual.policyeventsconstraints.dto;

import java.util.Date;

/**
 * PolicyPeriodDto is a data transfer object (DTO) that represents the period during which a policy is valid.
 * */
public class PolicyPeriodDto {
	private Date startDate;

	private Date endDate;

	public PolicyPeriodDto() {
	}

	public PolicyPeriodDto(Date startDate, Date endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public Date getStartDate() {
		return startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
}
