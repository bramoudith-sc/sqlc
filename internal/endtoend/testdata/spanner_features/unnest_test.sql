-- Test UNNEST functionality

-- name: TestBasicUnnest :many
-- Test basic UNNEST operation
SELECT value
FROM UNNEST(@arr) AS value;

-- name: TestUnnestWithOffset :many
-- Test UNNEST WITH OFFSET clause
SELECT value, offset_pos
FROM UNNEST(@tags) AS value WITH OFFSET AS offset_pos
WHERE offset_pos < 5;

-- name: TestUnnestInJoin :many
-- Test UNNEST in JOIN operation
SELECT u.name, tag
FROM users u
CROSS JOIN UNNEST(u.tags) AS tag
WHERE u.deleted_at IS NULL;

-- name: TestMultipleUnnest :many
-- Test multiple UNNEST operations
SELECT a, b
FROM UNNEST(@arr1) AS a
CROSS JOIN UNNEST(@arr2) AS b;

-- name: TestUnnestStruct :many
-- Test UNNEST with STRUCT array
SELECT item.id, item.name
FROM UNNEST(@items) AS item;

-- name: TestUnnestWithFilter :many
-- Test UNNEST with WHERE clause
SELECT value
FROM UNNEST(@values) AS value
WHERE value > @threshold;