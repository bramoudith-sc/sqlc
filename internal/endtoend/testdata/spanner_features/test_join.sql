-- Test LEFT JOIN with GROUP BY
-- name: TestLeftJoin :many
SELECT 
    u.id,
    u.name,
    COUNT(p.id) as post_count
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
GROUP BY u.id, u.name;