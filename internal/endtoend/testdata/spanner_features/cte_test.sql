-- Test CTE (WITH clause) support

-- name: TestSimpleCTE :many
WITH active_users AS (
  SELECT id, name 
  FROM users 
  WHERE deleted_at IS NULL
)
SELECT * FROM active_users;

-- name: TestCTEWithJoin :many
WITH 
  active_users AS (
    SELECT id, name FROM users WHERE deleted_at IS NULL
  ),
  recent_posts AS (
    SELECT user_id, title FROM posts WHERE status = 'published'
  )
SELECT 
  u.name,
  p.title
FROM active_users u
JOIN recent_posts p ON u.id = p.user_id;

-- name: TestExtractDate :one
SELECT 
  EXTRACT(YEAR FROM deleted_at) as year,
  EXTRACT(MONTH FROM deleted_at) as month,
  EXTRACT(DAY FROM deleted_at) as day
FROM users
WHERE id = @user_id;

-- name: TestSafeDivide :one
SELECT SAFE.DIVIDE(score, 10) as safe_score
FROM users
WHERE id = @user_id;

-- name: TestArrayAgg :one
SELECT ARRAY_AGG(name) as all_names
FROM users
WHERE deleted_at IS NULL;

-- name: TestStringAgg :one
SELECT STRING_AGG(name, ', ') as names_list
FROM users
WHERE deleted_at IS NULL;