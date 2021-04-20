package com.lakesidemutual.policyeventsconstraints;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = UpdatePolicyEvent.class, name = "UpdatePolicyEvent"),
        @JsonSubTypes.Type(value = DeletePolicyEvent.class, name = "DeletePolicyEvent")})
public interface PolicyDomainEvent extends DomainEvent {
    String policyId();
    String type();
}
