-- Test new expression implementations

-- name: TestUnaryNot :many
SELECT id, name
FROM users
WHERE NOT (score > 80);

-- name: TestUnaryMinus :one
SELECT -score as negative_score
FROM users
WHERE id = @user_id;

-- name: TestCountStar :one
SELECT COUNT(*) as total_users
FROM users;

-- name: TestCountStarGroupBy :many
SELECT status, COUNT(*) as count
FROM users
GROUP BY status;

-- name: TestBetween :many
SELECT id, name, score
FROM users
WHERE score BETWEEN 50 AND 80;

-- name: TestNotBetween :many
SELECT id, name, score
FROM users
WHERE score NOT BETWEEN 50 AND 80;

-- name: TestExtract :one
SELECT 
  EXTRACT(YEAR FROM created_at) as year,
  EXTRACT(MONTH FROM created_at) as month,
  EXTRACT(DAY FROM created_at) as day
FROM users
WHERE id = @user_id;

-- name: TestIfFunction :one
SELECT 
  IF(score >= 60, 'Pass', 'Fail') as result
FROM users
WHERE id = @user_id;

-- name: TestComplexExpression :one
SELECT 
  COUNT(*) as total,
  IF(COUNT(*) > 0, 'Has Users', 'No Users') as status
FROM users
WHERE NOT deleted_at IS NULL;