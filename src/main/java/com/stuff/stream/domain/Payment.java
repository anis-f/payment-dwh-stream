package com.stuff.stream.domain;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;



@Table(keyspace = "test", name = "payment")
public class Payment implements Serializable {

    @Column(name = "amount")
    private double amount;
    @Column(name = "currency")
    private String currency;
    @Column(name = "user_id")
    private String userId;
    @Column(name = "payee_id")
    private String payeeId;
    @Column(name = "payee_name")
    private String payeeName;
    @Column(name = "payment_method_id")
    private String paymentMethodId;
    @Column(name = "payment_method_name")
    private String paymentMethodName;
    @Column(name = "transaction_id")
    @PartitionKey(0)
    private String transactionId;
    @Column(name = "risk")
    private String risk;

    public Payment() {
        this.setTransactionId();
        this.setPayeeName();
        this.setPaymentMethodName();
    }

    public Payment(double amount, String currency, String userId, String payeeId, String paymentMethodId) {
        this.amount = amount;
        this.currency = currency;
        this.userId = userId;
        this.payeeId = payeeId;
        this.paymentMethodId = paymentMethodId;
        this.transactionId = UUID.randomUUID().toString();

        this.setTransactionId();
        this.setPayeeName();
        this.setPaymentMethodName();
    }

    public String getRisk() {
        return risk;
    }

    public void setRisk(String risk) {
        this.risk = risk;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setTransactionId() {
        this.setTransactionId(UUID.randomUUID().toString());
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setPayeeId(String payeeId) {
        this.payeeId = payeeId;
    }

    public void setPaymentMethodId(String paymentMethodId) {
        this.paymentMethodId = paymentMethodId;
    }

    public double getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getUserId() {
        return userId;
    }

    public String getPayeeId() {
        return payeeId;
    }

    public String getPaymentMethodId() {
        return paymentMethodId;
    }

    public String getPayeeName() {
        return payeeName;
    }

    public void setPayeeName(String payeeName) {
        this.payeeName = payeeName;
    }

    public void setPayeeName() {

        List<String> names = new ArrayList<>();
        names.add("John");
        names.add("Lisa");
        names.add("Alex");
        names.add("Abby");
        names.add("Barbra");
        names.add("Mike");
        int randomNameIndex = new Random().nextInt(names.size());
        String randomName = names.get(randomNameIndex);

        this.setPayeeName(randomName);
    }


    public String getPaymentMethodName() {
        return paymentMethodName;
    }

    public void setPaymentMethodName(String paymentMethodName) {
        this.paymentMethodName = paymentMethodName;
    }

    public void setPaymentMethodName() {

        List<String> paymentMethods = new ArrayList<>();
        paymentMethods.add("PayPal");
        paymentMethods.add("Cash");
        paymentMethods.add("Credit Card");
        paymentMethods.add("Bank Transfer");
        paymentMethods.add("Direct Deposit");
        paymentMethods.add("Google Pay");
        int randomPaymentMethodIndex = new Random().nextInt(paymentMethods.size());
        String randomPaymentMethod = paymentMethods.get(randomPaymentMethodIndex);

        this.setPaymentMethodName(randomPaymentMethod);

    }

    @Override
    public String toString() {
        return "Payment{" +
                "amount=" + amount +
                ", currency='" + currency + '\'' +
                ", userId='" + userId + '\'' +
                ", payeeId='" + payeeId + '\'' +
                ", payeeName='" + payeeName + '\'' +
                ", paymentMethodId='" + paymentMethodId + '\'' +
                ", paymentMethodName='" + paymentMethodName + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", risk='" + risk + '\'' +
                '}';
    }
}
