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

-- Test IFNULL function
-- name: GetUserNameOrDefault :one
SELECT IFNULL(name, 'Unknown User') as user_name
FROM users WHERE id = @user_id;

-- Test IFNULL with numbers
-- name: GetUserScoreOrZero :one
SELECT IFNULL(score, 100) as score_value
FROM users WHERE id = @user_id;

-- Test simple CASE with number in ELSE
-- name: TestCaseWithNumberElse :one
SELECT CASE WHEN score > 50 THEN score ELSE 0 END as result
FROM users WHERE id = @user_id;

-- Test NULLIF function
-- name: GetUserStatusNullIfDeleted :one
SELECT NULLIF(status, 'deleted') as active_status
FROM users WHERE id = @user_id;

-- Test complex COALESCE with multiple arguments
-- name: GetFirstNonNullValue :one
SELECT COALESCE(name, status, 'No Value') as first_value
FROM users WHERE id = @user_id;

-- Test COALESCE with numbers
-- name: GetUserScoreOrDefault :one
SELECT COALESCE(score, 0) as user_score
FROM users WHERE id = @user_id;

-- Test TypeCast directly
-- name: TestSimpleDateCast :one
SELECT DATE '2024-01-01' as date_value;

-- Test explicit CAST
-- name: TestExplicitCast :one
SELECT CAST('2024-01-01' AS DATE) as cast_date;

-- Debug: Test just returning a date column
-- name: TestDateColumn :one
SELECT deleted_at as date_col FROM users WHERE id = @user_id;

-- name: TestSimpleTimestampCast :one
SELECT TIMESTAMP '2024-01-01 10:00:00' as timestamp_value;

-- name: TestSimpleNumericCast :one
SELECT NUMERIC '123.456' as numeric_value;

-- Test all Spanner literal types with CASE expressions
-- name: TestIntegerLiteral :one
SELECT CASE WHEN true THEN 42 ELSE 0 END as int_value;

-- name: TestFloatLiteral :one
SELECT CASE WHEN true THEN 3.14 ELSE 0.0 END as float_value;

-- name: TestBooleanLiteral :one
SELECT CASE WHEN true THEN true ELSE false END as bool_value;

-- name: TestStringLiteral :one
SELECT CASE WHEN true THEN 'hello' ELSE 'world' END as string_value;

-- name: TestBytesLiteral :one
SELECT CASE WHEN true THEN b'hello' ELSE b'world' END as bytes_value;

-- name: TestDateLiteral :one
SELECT CASE WHEN true THEN DATE '2024-01-01' ELSE DATE '2024-12-31' END as date_value;

-- name: TestTimestampLiteral :one
SELECT CASE WHEN true THEN TIMESTAMP '2024-01-01 10:00:00' ELSE TIMESTAMP '2024-12-31 23:59:59' END as timestamp_value;

-- name: TestNumericLiteral :one
SELECT CASE WHEN true THEN NUMERIC '123.456' ELSE NUMERIC '0.0' END as numeric_value;

-- name: TestJsonLiteral :one
SELECT CASE WHEN true THEN JSON '{"key": "value"}' ELSE JSON '{}' END as json_value;

-- name: TestArrayLiteral :one
SELECT CASE WHEN true THEN [1, 2, 3] ELSE [4, 5, 6] END as array_value;

-- name: TestNullLiteral :one
SELECT CASE WHEN false THEN 'value' ELSE NULL END as null_value;

-- Test subquery support
-- name: TestScalarSubQuery :one
SELECT 
  u.name,
  (SELECT MAX(score) FROM users WHERE status = 'active') as max_score
FROM users u
WHERE u.id = @user_id;

-- name: TestExistsSubQuery :one
SELECT 
  u.id,
  u.name,
  EXISTS(SELECT 1 FROM posts p WHERE p.user_id = u.id) as has_posts
FROM users u
WHERE u.id = @user_id;

-- name: TestArraySubQuery :many
SELECT 
  u.id,
  u.name,
  ARRAY(SELECT p.title FROM posts p WHERE p.user_id = u.id) as post_titles
FROM users u;

-- Test STRUCT support
-- name: TestTypelessStruct :one
SELECT STRUCT(1, 'hello', true) as struct_value;

-- name: TestTypedStruct :one
SELECT STRUCT<x INT64, y STRING, z BOOL>(42, 'world', false) as typed_struct;

-- name: TestTupleStruct :one
SELECT (100, 'tuple', DATE '2024-01-01') as tuple_value;

-- Test INTERVAL support  
-- name: TestIntervalSingle :one
SELECT INTERVAL 5 DAY as interval_days;

-- name: TestIntervalRange :one
SELECT INTERVAL '1-2' YEAR TO MONTH as interval_range;

-- Test array index access
-- name: TestArrayIndexAccess :one
SELECT 
  ['apple', 'banana', 'cherry'][1] as second_fruit,
  ARRAY<INT64>[10, 20, 30][OFFSET(0)] as first_number;

-- Test struct field access
-- name: TestStructFieldAccess :one
SELECT 
  STRUCT(1 as id, 'John' as name).name as person_name;

-- name: TestStructFieldAccess2 :one
SELECT 
  STRUCT<id INT64, name STRING>(42, 'Alice').name as typed_name;