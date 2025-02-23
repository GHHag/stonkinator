package entities

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
	Id       string
	RoleId   string
	Role     string
	Username string
	Password string
}

func (u *User) Register(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
		defer cancel()

		err := r.ParseForm()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user := User{
			Username: r.Form.Get("username"),
			Password: r.Form.Get("password"),
		}

		// encrypt(&user.Password)
		// user.Password = encrypt(user.Password)

		result, err := pgPool.Exec(
			ctx,
			`
				INSERT INTO users(user_role_id, username, user_password)
				VALUES(
					(
						SELECT id
						FROM user_roles
						WHERE role_name = 'user'
					), 
					$1, $2
				)
				ON CONFLICT DO NOTHING
			`,
			user.Username, user.Password,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rowsAffected := result.RowsAffected()
		if rowsAffected == 0 {
			http.Error(w, "Failed to register user", http.StatusConflict)
			return
		}

		// TODO: Create token/session with user data and set as cookie or header

		// w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		// http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

func (u *User) Login(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
		defer cancel()

		err := r.ParseForm()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		suppliedUser := User{
			Username: r.Form.Get("username"),
			Password: r.Form.Get("password"),
		}

		query := pgPool.QueryRow(
			ctx,
			`
				SELECT users.id, users.username, users.user_password, user_roles.role_name
				FROM users, user_roles
				WHERE user_roles.id = users.user_role_id
				AND users.username = $1
			`,
			suppliedUser.Username,
		)

		var user User
		err = query.Scan(&user.Id, &user.Username, &user.Password, &user.Role)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// decrypt(&user.Password)
		// user.Password = decrypt(user.Password)
		// passwordValid := validatePassword(suppliedUser.Password, user.Password)
		// if passwordValid != true {
		// 	http.Error(w, "Invalid credentials", http.StatusInternalServerError)
		// 	http.Redirect(w, r, "/login", http.StatusSeeOther)
		// 	return
		// }

		// TODO: Create token/session with user data and set as cookie or header

		// w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

func (u *User) Logout(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}
