-- Test DotStar syntax

-- name: TestSimpleDotStar :many
-- Test basic table.* syntax
SELECT u.*
FROM users u
WHERE u.deleted_at IS NULL;

-- name: TestDotStarWithColumns :many
-- Test table.* with additional columns
SELECT u.*, p.title
FROM users u
JOIN posts p ON u.id = p.user_id
WHERE u.deleted_at IS NULL;