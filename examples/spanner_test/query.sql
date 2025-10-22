-- name: GetUser :one
SELECT id, name, email FROM users WHERE id = @user_id;

-- name: ListUsers :many
SELECT id, name, email FROM users ORDER BY name;

-- name: ListUsersStarTest :many  
SELECT * FROM users ORDER BY name;

-- name: CreateUser :exec
INSERT INTO users (id, name, email) VALUES (@id, @name, @email);

-- name: UpdateUser :exec
UPDATE users SET name = @name, email = @email WHERE id = @id;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = @id;

-- name: CreateUserReturning :one
INSERT INTO users (id, name, email) VALUES (@id, @name, @email) THEN RETURN id, name, email;