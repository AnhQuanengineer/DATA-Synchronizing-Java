CREATE TABLE IF NOT EXISTS user_log_before(
    user_id BIGINT,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);
CREATE TABLE IF NOT EXISTS user_log_after(
    user_id BIGINT,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

DELIMITER //
CREATE TRIGGER before_update_users
BEFORE UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "UPDATE");
END //

CREATE TRIGGER before_delete_users
BEFORE DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END //

CREATE TRIGGER after_update_users
AFTER UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "UPDATE");
END //

CREATE TRIGGER after_insert_users
AFTER INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END //

DELIMITER ;


INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES
(1, 'alice123', 'abc123', 'https://example.com/avatar1.png', 'https://example.com/users/alice123'),
(2, 'bob456', 'def456', 'https://example.com/avatar2.png', 'https://example.com/users/bob456'),
(3, 'charlie789', 'ghi789', 'https://example.com/avatar3.png', 'https://example.com/users/charlie789'),
(4, 'david321', 'jkl321', 'https://example.com/avatar4.png', 'https://example.com/users/david321'),
(5, 'eve654', 'mno654', 'https://example.com/avatar5.png', 'https://example.com/users/eve654'),
(6, 'frank987', 'pqr987', 'https://example.com/avatar6.png', 'https://example.com/users/frank987'),
(7, 'grace111', 'stu111', 'https://example.com/avatar7.png', 'https://example.com/users/grace111'),
(8, 'henry222', 'vwx222', 'https://example.com/avatar8.png', 'https://example.com/users/henry222'),
(9, 'ivy333', 'yz333', 'https://example.com/avatar9.png', 'https://example.com/users/ivy333'),
(10, 'jack444', 'aaa444', 'https://example.com/avatar10.png', 'https://example.com/users/jack444');



INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES
(11, 'kate555', 'bbb555', 'https://example.com/avatar11.png', 'https://example.com/users/kate555'),
(12, 'leo666', 'ccc666', 'https://example.com/avatar12.png', 'https://example.com/users/leo666'),
(13, 'mia777', 'ddd777', 'https://example.com/avatar13.png', 'https://example.com/users/mia777'),
(14, 'nick888', 'eee888', 'https://example.com/avatar14.png', 'https://example.com/users/nick888'),
(15, 'olivia999', 'fff999', 'https://example.com/avatar15.png', 'https://example.com/users/olivia999'),
(16, 'paul1010', 'ggg1010', 'https://example.com/avatar16.png', 'https://example.com/users/paul1010'),
(17, 'quinn1111', 'hhh1111', 'https://example.com/avatar17.png', 'https://example.com/users/quinn1111'),
(18, 'ruby1212', 'iii1212', 'https://example.com/avatar18.png', 'https://example.com/users/ruby1212'),
(19, 'sam1313', 'jjj1313', 'https://example.com/avatar19.png', 'https://example.com/users/sam1313'),
(20, 'tina1414', 'kkk1414', 'https://example.com/avatar20.png', 'https://example.com/users/tina1414');


INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES
(21, 'ursula1515', 'lll1515', 'https://example.com/avatar21.png', 'https://example.com/users/ursula1515'),
(22, 'victor1616', 'mmm1616', 'https://example.com/avatar22.png', 'https://example.com/users/victor1616'),
(23, 'wendy1717', 'nnn1717', 'https://example.com/avatar23.png', 'https://example.com/users/wendy1717'),
(24, 'xavier1818', 'ooo1818', 'https://example.com/avatar24.png', 'https://example.com/users/xavier1818'),
(25, 'yasmin1919', 'ppp1919', 'https://example.com/avatar25.png', 'https://example.com/users/yasmin1919'),
(26, 'zack2020', 'qqq2020', 'https://example.com/avatar26.png', 'https://example.com/users/zack2020'),
(27, 'amy2121', 'rrr2121', 'https://example.com/avatar27.png', 'https://example.com/users/amy2121'),
(28, 'brian2222', 'sss2222', 'https://example.com/avatar28.png', 'https://example.com/users/brian2222'),
(29, 'clara2323', 'ttt2323', 'https://example.com/avatar29.png', 'https://example.com/users/clara2323'),
(30, 'daniel2424', 'uuu2424', 'https://example.com/avatar30.png', 'https://example.com/users/daniel2424');


INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES
(31, 'ella2525', 'vvv2525', 'https://example.com/avatar31.png', 'https://example.com/users/ella2525'),
(32, 'felix2626', 'www2626', 'https://example.com/avatar32.png', 'https://example.com/users/felix2626'),
(33, 'gina2727', 'xxx2727', 'https://example.com/avatar33.png', 'https://example.com/users/gina2727'),
(34, 'hugo2828', 'yyy2828', 'https://example.com/avatar34.png', 'https://example.com/users/hugo2828'),
(35, 'iris2929', 'zzz2929', 'https://example.com/avatar35.png', 'https://example.com/users/iris2929'),
(36, 'jason3030', 'aaa3030', 'https://example.com/avatar36.png', 'https://example.com/users/jason3030'),
(37, 'karen3131', 'bbb3131', 'https://example.com/avatar37.png', 'https://example.com/users/karen3131'),
(38, 'liam3232', 'ccc3232', 'https://example.com/avatar38.png', 'https://example.com/users/liam3232'),
(39, 'maya3333', 'ddd3333', 'https://example.com/avatar39.png', 'https://example.com/users/maya3333'),
(40, 'noah3434', 'eee3434', 'https://example.com/avatar40.png', 'https://example.com/users/noah3434');


INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES
(41, 'oliver3535', 'fff3535', 'https://example.com/avatar41.png', 'https://example.com/users/oliver3535'),
(42, 'penny3636', 'ggg3636', 'https://example.com/avatar42.png', 'https://example.com/users/penny3636'),
(43, 'quentin3737', 'hhh3737', 'https://example.com/avatar43.png', 'https://example.com/users/quentin3737'),
(44, 'rachel3838', 'iii3838', 'https://example.com/avatar44.png', 'https://example.com/users/rachel3838'),
(45, 'steve3939', 'jjj3939', 'https://example.com/avatar45.png', 'https://example.com/users/steve3939'),
(46, 'tina4040', 'kkk4040', 'https://example.com/avatar46.png', 'https://example.com/users/tina4040'),
(47, 'ursula4141', 'lll4141', 'https://example.com/avatar47.png', 'https://example.com/users/ursula4141'),
(48, 'vincent4242', 'mmm4242', 'https://example.com/avatar48.png', 'https://example.com/users/vincent4242'),
(49, 'wanda4343', 'nnn4343', 'https://example.com/avatar49.png', 'https://example.com/users/wanda4343'),
(50, 'xena4444', 'ooo4444', 'https://example.com/avatar50.png', 'https://example.com/users/xena4444');


INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES
(51, 'yuri4545', 'ppp4545', 'https://example.com/avatar51.png', 'https://example.com/users/yuri4545'),
(52, 'zoe4646', 'qqq4646', 'https://example.com/avatar52.png', 'https://example.com/users/zoe4646'),
(53, 'adam4747', 'rrr4747', 'https://example.com/avatar53.png', 'https://example.com/users/adam4747'),
(54, 'bella4848', 'sss4848', 'https://example.com/avatar54.png', 'https://example.com/users/bella4848'),
(55, 'carl4949', 'ttt4949', 'https://example.com/avatar55.png', 'https://example.com/users/carl4949'),
(56, 'diana5050', 'uuu5050', 'https://example.com/avatar56.png', 'https://example.com/users/diana5050'),
(57, 'edward5151', 'vvv5151', 'https://example.com/avatar57.png', 'https://example.com/users/edward5151'),
(58, 'fiona5252', 'www5252', 'https://example.com/avatar58.png', 'https://example.com/users/fiona5252'),
(59, 'george5353', 'xxx5353', 'https://example.com/avatar59.png', 'https://example.com/users/george5353'),
(60, 'hannah5454', 'yyy5454', 'https://example.com/avatar60.png', 'https://example.com/users/hannah5454');



