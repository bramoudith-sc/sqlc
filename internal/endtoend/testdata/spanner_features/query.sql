-- Test CASE WHEN expressions
-- name: GetUserGrade :one
SELECT 
  name,
  CASE 
    WHEN score >= 90 THEN 'Excellent'
    WHEN score >= 70 THEN 'Good'
    WHEN score >= 50 THEN 'Pass'
    ELSE 'Fail'
  END as grade
FROM users WHERE id = @user_id;

-- Test CAST operations
-- name: GetUserIdAsInt :one
SELECT CAST(id AS INT64) as numeric_id
FROM users WHERE id = @user_id;

-- Test COALESCE function
-- name: GetUserDisplayName :one
SELECT COALESCE(name, 'Anonymous') as display_name
FROM users WHERE id = @user_id;

-- Test IS NULL/IS NOT NULL
-- name: GetActiveUsers :many
SELECT id, name 
FROM users 
WHERE deleted_at IS NULL;

-- name: GetDeletedUsers :many
SELECT id, name
FROM users
WHERE deleted_at IS NOT NULL;

-- Test IN operator
-- name: GetUsersByStatus :many
SELECT id, name
FROM users
WHERE status IN ('active', 'pending', 'verified');

-- Test JOIN operations
-- name: GetUserWithPosts :many
SELECT 
    u.id as user_id,
    u.name as user_name,
    p.id as post_id,
    p.title as post_title
FROM users u
INNER JOIN posts p ON u.id = p.user_id
WHERE u.deleted_at IS NULL;

-- Test LEFT JOIN
-- name: GetUsersWithPostCount :many
SELECT 
    u.id,
    u.name,
    COUNT(p.id) as post_count
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
GROUP BY u.id, u.name;