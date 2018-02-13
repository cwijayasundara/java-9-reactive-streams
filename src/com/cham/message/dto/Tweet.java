package com.cham.message.dto;

public class Tweet {

    private int messageId;
    private String userName;
    private String message;

    public Tweet(int messageId, String userName, String message){
        this.messageId = messageId;
        this.userName=userName;
        this.message= message;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "messageId=" + messageId +
                ", userName='" + userName + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
