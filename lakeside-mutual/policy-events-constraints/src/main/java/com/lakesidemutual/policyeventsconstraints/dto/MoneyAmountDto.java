package com.lakesidemutual.policyeventsconstraints.dto;

import java.math.BigDecimal;

/**
 * MoneyAmountDto is a data transfer object (DTO) that represents an amount of money in a specific currency.
 */
public class MoneyAmountDto {
	private BigDecimal amount;

	private String currency;

	public MoneyAmountDto() {
	}

	public MoneyAmountDto(BigDecimal amount, String currency) {
		this.amount = amount;
		this.currency = currency;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public String getCurrency() {
		return currency;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}
}
