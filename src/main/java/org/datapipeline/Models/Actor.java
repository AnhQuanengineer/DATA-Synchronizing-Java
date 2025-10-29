package org.datapipeline.Models;

import java.io.Serializable;

public class Actor implements Serializable {
    private Long id;
    private String login;
    private String gravatar_id; // Đặt tên khớp với field trong JSON
    private String url;
    private String avatar_url; // Đặt tên khớp với field trong JSON

    // Constructors
    public Actor() {}

    public Actor(Long id, String login, String gravatar_id, String url, String avatar_url) {
        this.id = id;
        this.login = login;
        this.gravatar_id = gravatar_id;
        this.url = url;
        this.avatar_url = avatar_url;
    }

    // Getters và Setters (Bắt buộc)
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getLogin() { return login; }
    public void setLogin(String login) { this.login = login; }
    public String getGravatar_id() { return gravatar_id; }
    public void setGravatar_id(String gravatar_id) { this.gravatar_id = gravatar_id; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getAvatar_url() { return avatar_url; }
    public void setAvatar_url(String avatar_url) { this.avatar_url = avatar_url; }
}
