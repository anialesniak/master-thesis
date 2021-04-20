package com.lakesidemutual.policyeventsconstraints.dto;

import java.util.List;

/**
 * InsuringAgreementDto is a data transfer object (DTO) that represents the
 * insuring agreement between a customer and Lakeside Mutual.
 */
public class InsuringAgreementDto {

	private final List<InsuringAgreementItemDto> agreementItems;

	public InsuringAgreementDto() {
		this.agreementItems = null;
	}

	public InsuringAgreementDto(List<InsuringAgreementItemDto> agreementItems) {
		this.agreementItems = agreementItems;
	}

	public List<InsuringAgreementItemDto> getAgreementItems() {
		return agreementItems;
	}
}
