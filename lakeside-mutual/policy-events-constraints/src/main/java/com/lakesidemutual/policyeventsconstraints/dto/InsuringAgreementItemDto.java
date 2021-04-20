package com.lakesidemutual.policyeventsconstraints.dto;

/**
 * InsuringAgreementItemDto is a data transfer object (DTO) that is used to represent a single item in an insuring agreement.
 */
public class InsuringAgreementItemDto {
	private final String title;

	private final String description;

	public InsuringAgreementItemDto() {
		this.title = null;
		this.description = null;
	}

	private InsuringAgreementItemDto(String title, String description) {
		this.title = title;
		this.description = description;
	}

	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}
}
