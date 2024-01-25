package com.atguigu.bean;

// 定义订单数据类

public class Order {
    public String orderId;
    public String userId;
    public String merchantId;
    public double amount;

    public Order() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public Order(String orderId, String userId, String merchantId, double amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.merchantId = merchantId;
        this.amount = amount;
    }


    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
