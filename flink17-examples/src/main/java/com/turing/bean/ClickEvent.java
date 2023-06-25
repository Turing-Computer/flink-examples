package com.turing.bean;

import java.sql.Timestamp;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-02-28
 */
public class ClickEvent {

    public String user;

    public String url;

    public Long timestamp;

    //无参构造方法
    public ClickEvent() {
    }

    public ClickEvent(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
