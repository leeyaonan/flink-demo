package com.leeyaonan.chapter05;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 14:26
 * @desc: 日志记录类
 */
public class Event {

    /**
     * 用户
     */
    public String user;

    /**
     * 访问url
     */
    public String url;

    /**
     * 时间戳
     */
    public Long timestamp;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
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

    public static Event createRandomEvent() {
        return new Event(getRandomUser(), getRandomUrl(), getRandomTimestamp());
    }

    private static String getRandomUser() {
        List<String> list = Arrays.asList("Tom", "Zay", "Tony", "Rot", "Millie", "Billie", "Josie", "Jerry", "Jim", "Maybe");
        return list.get(new Random().nextInt(list.size()));
    }

    private static String getRandomUrl() {
        List<String> list = Arrays.asList("./home", "./login", "./list", "./detail", "./sign", "./logout");
        List<String> resultList = new ArrayList<>(list);
//        for (int i = 0; i < 10; i++) {
//            resultList.add("./product?id=" + i);
//        }
        return resultList.get(new Random().nextInt(resultList.size()));
    }

    private static Long getRandomTimestamp() {
        return new Date().getTime();
    }

    public static List<Event> createTestEventList(int i) {
        List<Event> list = new ArrayList<>();
        for (int j = 0; j < i; j++) {
            list.add(Event.createRandomEvent());
        }
        return list;
    }
}
