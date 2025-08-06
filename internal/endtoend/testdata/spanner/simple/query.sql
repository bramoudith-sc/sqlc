-- name: GetUser :one
SELECT * FROM users WHERE id = @user_id;

-- name: CreateUser :one  
INSERT INTO users (id, name, email) VALUES (@id, @name, @email)
THEN RETURN *;