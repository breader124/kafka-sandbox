package udemy.breader.com.assignment.balance;

public enum Topic {
    USER_DATA("user-data"),
    USER_PURCHASE("user-purchase"),
    INNER_JOINED_DATA_PURCHASE("inner-joined-data-purchase"),
    LEFT_JOINED_DATA_PURCHASE("left-joined-data-purchase");

    Topic(String topicName) {
        this.topicName = topicName;
    }

    private final String topicName;

    public String getTopicName() {
        return topicName;
    }
}
