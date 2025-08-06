CREATE TABLE authors (
  id INT64 NOT NULL,
  name STRING(100) NOT NULL,
  bio STRING(MAX),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP,
) PRIMARY KEY (id);

CREATE TABLE books (
  id INT64 NOT NULL,
  author_id INT64 NOT NULL,
  title STRING(200) NOT NULL,
  description STRING(MAX),
  price NUMERIC,
  published_date DATE,
  metadata JSON,
  tags ARRAY<STRING(50)>,
  available BOOL NOT NULL DEFAULT (true),
  CONSTRAINT FK_BookAuthor FOREIGN KEY (author_id) REFERENCES authors (id),
) PRIMARY KEY (id);

CREATE INDEX idx_books_author ON books(author_id);
CREATE INDEX idx_books_published ON books(published_date);