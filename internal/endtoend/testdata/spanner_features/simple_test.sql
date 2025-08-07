-- name: SimpleCase :one
SELECT 
  CASE 
    WHEN score >= 90 THEN 'A'
    ELSE 'B'
  END as grade
FROM users WHERE id = @user_id;