CREATE TABLE users (
    id STRING(36) NOT NULL,
    name STRING(100),
    email STRING(255),
    score INT64,
    status STRING(20),
    deleted_at TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE posts (
    id STRING(36) NOT NULL,
    user_id STRING(36) NOT NULL,
    title STRING(200),
    status STRING(20),
    published_at TIMESTAMP
) PRIMARY KEY (id);