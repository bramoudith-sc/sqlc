-- name: GetAuthor :one
SELECT * FROM authors
WHERE id = @author_id;

-- name: ListAuthors :many
SELECT * FROM authors
ORDER BY name;

-- name: CreateAuthor :one
INSERT INTO authors (
  id, name, bio, created_at
) VALUES (
  @id, @name, @bio, CURRENT_TIMESTAMP()
)
THEN RETURN *;

-- name: UpdateAuthor :one
UPDATE authors
SET name = @name,
    bio = @bio,
    updated_at = CURRENT_TIMESTAMP()
WHERE id = @id
THEN RETURN *;

-- name: DeleteAuthor :exec
DELETE FROM authors
WHERE id = @id;

-- name: GetBook :one
SELECT * FROM books
WHERE id = @book_id;

-- name: ListBooks :many
SELECT * FROM books
ORDER BY title;

-- name: ListBooksByAuthor :many
SELECT * FROM books
WHERE author_id = @author_id
ORDER BY published_date DESC;

-- name: CreateBook :one
INSERT INTO books (
  id, author_id, title, description, price, published_date, metadata, tags, available
) VALUES (
  @id, @author_id, @title, @description, @price, @published_date, @metadata, @tags, @available
)
THEN RETURN *;

-- name: SearchBooks :many
SELECT b.*, a.name as author_name
FROM books b
JOIN authors a ON b.author_id = a.id
WHERE LOWER(b.title) LIKE LOWER(@search_term)
   OR LOWER(b.description) LIKE LOWER(@search_term)
ORDER BY b.published_date DESC;

-- name: UpdateBookPrice :exec
UPDATE books
SET price = SAFE_ADD(price, @price_increase)
WHERE id = @book_id;

-- name: GetBooksWithTags :many
SELECT * FROM books
WHERE ARRAY_INCLUDES(tags, @tag)
ORDER BY title;

-- name: GetRecentBooks :many
SELECT * FROM books
WHERE published_date >= DATE_SUB(CURRENT_DATE(), @days_ago)
ORDER BY published_date DESC;

-- name: GetBookStats :one
SELECT 
  COUNT(*) as total_books,
  COUNT(DISTINCT author_id) as total_authors,
  AVG(price) as avg_price,
  MIN(published_date) as earliest_published,
  MAX(published_date) as latest_published
FROM books;

-- name: GetAuthorBookCount :many
SELECT 
  a.id,
  a.name,
  COUNT(b.id) as book_count,
  ARRAY_AGG(b.title ORDER BY b.published_date DESC LIMIT 5) as recent_titles
FROM authors a
LEFT JOIN books b ON a.id = b.author_id
GROUP BY a.id, a.name
ORDER BY book_count DESC;