package com.bigdata.flink.bean;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/9/27 13:27
 */
public class User {
    private Integer id;
    private String name;
    private Integer age;
    private String addr;

    public static User of(Integer id, String name, Integer age, String addr){
        return new User(id, name, age, addr);
    }

    public User(Integer id, String name, Integer age, String addr) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.addr = addr;
    }

    public User() {
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", addr='" + addr + '\'' +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }
}
