package main

import (
	"database/sql"
	"net/http"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type DbExplorer struct {
	db *sql.DB
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	return &DbExplorer{db: db}, nil
}

func (*DbExplorer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

}
