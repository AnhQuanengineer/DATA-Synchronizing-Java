package org.datapipeline.Models;

import java.io.Serializable;

public class GithubEvent implements Serializable {
    // Lưu ý: Nếu bạn sử dụng Schema đầy đủ (bao gồm id, type, created_at),
    // bạn phải thêm các field đó vào lớp này.

    private Actor actor;
    private Repo repo;

    // Constructors
    public GithubEvent() {}

    public GithubEvent(Actor actor, Repo repo) {
        this.actor = actor;
        this.repo = repo;
    }

    // Getters và Setters (Bắt buộc)
    public Actor getActor() { return actor; }
    public void setActor(Actor actor) { this.actor = actor; }
    public Repo getRepo() { return repo; }
    public void setRepo(Repo repo) { this.repo = repo; }
}
