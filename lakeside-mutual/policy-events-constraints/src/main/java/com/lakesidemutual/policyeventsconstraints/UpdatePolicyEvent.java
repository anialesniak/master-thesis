package com.lakesidemutual.policyeventsconstraints;

import java.util.Date;

import com.lakesidemutual.policyeventsconstraints.dto.CustomerDto;
import com.lakesidemutual.policyeventsconstraints.dto.PolicyDto;

/**
 * UpdatePolicyEvent is a domain event that is sent to the Risk Management Server
 * every time a new policy is created or an existing policy is updated.
 * */
public class UpdatePolicyEvent implements PolicyDomainEvent {
    private String kind;
    private String originator;
    private Date date;
    private CustomerDto customer;
    private PolicyDto policy;

    public UpdatePolicyEvent() {
    }

    public UpdatePolicyEvent(String originator, Date date, CustomerDto customer, PolicyDto policy) {
        this.kind = "UpdatePolicyEvent";
        this.originator = originator;
        this.date = date;
        this.customer = customer;
        this.policy = policy;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getOriginator() {
        return originator;
    }

    public void setOriginator(String originator) {
        this.originator = originator;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public CustomerDto getCustomer() {
        return customer;
    }

    public void setCustomer(CustomerDto customer) {
        this.customer = customer;
    }

    public PolicyDto getPolicy() {
        return policy;
    }

    public void setPolicy(PolicyDto policy) {
        this.policy = policy;
    }

    @Override
    public String policyId() {
        return policy.getPolicyId();
    }

    @Override
    public String type() {
        return kind;
    }
}
