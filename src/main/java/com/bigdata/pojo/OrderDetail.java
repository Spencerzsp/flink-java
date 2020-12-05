package com.bigdata.pojo;

import java.util.Date;

/**
 * @ author spencer
 * @ date 2020/6/11 10:51
 */
public class OrderDetail {

    private Long id;
    private Long order_id;
    private int category_id;
    private String categoryName;
    private Long sku;
    private Double money;
    private int amount;
    private Date create_time;
    private Date update_time;

    private String type;

    @Override
    public String toString() {
        return "OrderDetail{" +
                "id=" + id +
                ", order_id=" + order_id +
                ", category_id=" + category_id +
                ", categoryName='" + categoryName + '\'' +
                ", sku=" + sku +
                ", money=" + money +
                ", amount=" + amount +
                ", create_time=" + create_time +
                ", update_time=" + update_time +
                ", type='" + type + '\'' +
                '}';
    }

    public OrderDetail() {
    }

    public OrderDetail(Long id, Long order_id, int category_id, String categoryName, Long sku, Double money, int amount, Date create_time, Date update_time, String type) {
        this.id = id;
        this.order_id = order_id;
        this.category_id = category_id;
        this.categoryName = categoryName;
        this.sku = sku;
        this.money = money;
        this.amount = amount;
        this.create_time = create_time;
        this.update_time = update_time;
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public Long getSku() {
        return sku;
    }

    public void setSku(Long sku) {
        this.sku = sku;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
