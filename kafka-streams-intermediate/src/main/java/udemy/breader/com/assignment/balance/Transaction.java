package udemy.breader.com.assignment.balance;

public class Transaction {
    String creditor;
    Integer amount;
    String isoTime;

    public Transaction(String creditor, Integer amount, String time) {
        this.creditor = creditor;
        this.amount = amount;
        this.isoTime = time;
    }
}
