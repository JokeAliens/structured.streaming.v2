/*
 * Copyright (c) 2021. JokeAliens
 */

package hello.jokealiens.cn.bean;

import java.io.Serializable;

/**
 * @author JokeAlien
 * @date 2021/3/25
 */
public class RawStatusBean implements Serializable {
    private final long time;
    private final String status = "ACTIVE";
    private String content;

    public RawStatusBean(String content) {
        this.time = System.currentTimeMillis();
        this.content = content;
    }

    public long getTime() {
        return time;
    }

    public String getStatus() {
        return status;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
