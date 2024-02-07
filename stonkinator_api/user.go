package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type User struct {
	Id           string `json:"id,omitempty"`
	UserRoleId   string `json:"userRoleId,omitempty"`
	Username     string `json:"username"`
	UserPassword string `json:"password,omitempty"`
}

func RegisterUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result, err := pgPool.Exec(
		context.Background(),
		`
			INSERT INTO users(user_role_id, username, user_password)
			(
				SELECT id, $1, $2
				FROM user_roles
				WHERE role_name = 'user'
			)
			ON CONFLICT DO NOTHING RETURNING id
		`,
		user.Username, user.UserPassword,
	)
	if err != nil {
		http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected() == 0 {
		http.Error(w, "Failed to create user", http.StatusConflict)
		return
	}

	// get the id from registered user and return as json

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, "User created")
}
