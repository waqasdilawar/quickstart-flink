package org.myorg.quickstart.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageEvent {

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("message_body")
    private String messageBody;

    @JsonProperty("correlation_id")
    private String correlationId;

    @JsonProperty("message_status")
    private String messageStatus;

    @JsonProperty("timestamp")
    private String timestamp;

    public enum ProfanityType {
        PROFANITY,
        SAFE
    }

    @JsonProperty("profanity_type")
    private ProfanityType profanityType;

    // Default constructor
    public MessageEvent() {
    }

    // Getters and Setters
    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageBody() {
        return messageBody;
    }

    public void setMessageBody(String messageBody) {
        this.messageBody = messageBody;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getMessageStatus() {
        return messageStatus;
    }

    public void setMessageStatus(String messageStatus) {
        this.messageStatus = messageStatus;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public ProfanityType getProfanityType() {
        return profanityType;
    }

    public void setProfanityType(ProfanityType profanityType) {
        this.profanityType = profanityType;
    }
}
