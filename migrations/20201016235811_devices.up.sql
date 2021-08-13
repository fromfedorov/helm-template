CREATE TABLE IF NOT EXISTS requests (
    id SERIAL,
    name VARCHAR(256),
    description VARCHAR(2048),
    video_url VARCHAR(64),
    text_url VARCHAR(64),

    processed BOOL DEFAULT FALSE,
    archived BOOL DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT null,

    UNIQUE(name)
);
