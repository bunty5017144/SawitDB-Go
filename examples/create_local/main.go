package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"github.com/WowoEngine/SawitDB-Go/internal/engine"
)

func main() {
	dbPath := "example.sawit"
	if _, err := os.Stat(dbPath); err == nil {
		os.Remove(dbPath)
	}

	// Create absolute path to avoid directory confusion
	absPath, _ := filepath.Abs(dbPath)

	db, err := engine.NewSawitDB(absPath)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	fmt.Println("Generating example.sawit...")

	// 1. Create Tables
	// Note: WowoEngine direct usage creates tables inside the single DB file "example.sawit" wrapper?
	// Wait, SawitServer maps "database name" to "filename.sawit".
	// WowoEngine operates ON "filename.sawit".
	// "BUKA WILAYAH" or "CREATE TABLE" inside engine.
	// In Node.js create-local.js: `db.query("LAHAN karet")`.
	// WowoEngine.go: `createTable` (LAHAN/CREATE TABLE).

	query(db, "LAHAN karet")
	query(db, "LAHAN sawit")
	query(db, "LAHAN kopi")

	// 2. Insert Data
	query(db, "TANAM KE karet (id, jenis, lokasi) BIBIT (1, 'GT1', 'Blok A')")
	query(db, "TANAM KE karet (id, jenis, lokasi) BIBIT (2, 'PB260', 'Blok A')")

	query(db, "TANAM KE sawit (id, bibit, umur) BIBIT (101, 'Dura', 2)")
	query(db, "TANAM KE sawit (id, bibit, umur) BIBIT (102, 'Tenera', 5)")
	query(db, "TANAM KE sawit (id, bibit, umur) BIBIT (103, 'Pisifera', 1)")

	query(db, "TANAM KE kopi (kode, varietas) BIBIT ('K01', 'Robusta')")
	query(db, "TANAM KE kopi (kode, varietas) BIBIT ('K02', 'Arabika')")

	fmt.Println("\n--- VERIFICATION TEST ---")
	printQuery(db, "Karet:", "PANEN * DARI karet")
	printQuery(db, "Sawit:", "PANEN * DARI sawit")
	printQuery(db, "Kopi:", "PANEN * DARI kopi")
}

func query(db *engine.SawitDB, q string) {
	res, err := db.Query(q, nil)
	if err != nil {
		fmt.Printf("Query error '%s': %v\n", q, err)
	} else {
		fmt.Printf("Query '%s': %v\n", q, res)
	}
}

func printQuery(db *engine.SawitDB, label, q string) {
	res, err := db.Query(q, nil)
	if err != nil {
		fmt.Printf("%s Error: %v\n", label, err)
		return
	}
	b, _ := json.Marshal(res)
	fmt.Printf("%s %s\n", label, string(b))
}
