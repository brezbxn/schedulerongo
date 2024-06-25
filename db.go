package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"os"
)

/*
	 func initDBConnection() *sql.DB {
		connStr := os.Getenv("DB_DSN")
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Panic("couldn't connect to database", err)
		}
		return db
	}
*/
func initDBConnection() *sql.DB {
	conn := os.Getenv("POSTGRES_CONNECTION")
	password := os.Getenv("POSTGRES_PASSWORD")
	if conn == "" || password == "" {
		conn = "postgres"
	}
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func seedDB(db *sql.DB) error {
	log.Print("Seeding database with table. . .")
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS "public"."jobs" (
    	"id" SERIAL PRIMARY KEY,
    	"name" varchar(50) NOT NULL,
    	"payload" text,
 		"runAt" TIMESTAMP NOT NULL,
    	"cron" varchar(50) DEFAULT '-'
)
`)
	if err != nil {
		log.Panic("query error:", err)
	}
	return err
}
