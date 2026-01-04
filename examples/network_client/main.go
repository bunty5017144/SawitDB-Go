package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sawitdb/pkg/client"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║   🌴 SawitDB Client - Example Usage            ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")

	// Connect to server
	// Format: sawitdb://[username:password@]host:port/database
	c := client.NewSawitClient("sawitdb://localhost:7878/plantation")

	fmt.Println("[1] Connecting to SawitDB server...")
	if err := c.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	fmt.Println("✓ Connected!")

	// Create table
	fmt.Println("[2] Creating table...")
	res, err := c.Query("LAHAN sawit_block_a", nil)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Result: %v\n", res)
	}
	fmt.Println("")

	// Insert data
	fmt.Println("[3] Inserting data...")
	queries := []string{
		"TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (1, 'Tenera', 5, 120)",
		"TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (2, 'Dura', 3, 80)",
		"TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (3, 'Tenera', 7, 150)",
		"TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (4, 'Pisifera', 2, 60)",
		"TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (5, 'Tenera', 10, 200)",
	}

	for _, q := range queries {
		_, err := c.Query(q, nil)
		if err != nil {
			log.Fatalf("Insert failed: %v", err)
		}
	}
	fmt.Println("✓ Inserted 5 records")

	// Select all
	fmt.Println("[4] Selecting all data...")
	printQuery(c, "PANEN * DARI sawit_block_a")

	// Select with WHERE
	fmt.Println("[5] Selecting with WHERE clause (umur > 5)...")
	printQuery(c, "PANEN * DARI sawit_block_a DIMANA umur > 5")

	// Select with AND/OR
	fmt.Println("[6] Selecting with AND condition...")
	printQuery(c, "PANEN * DARI sawit_block_a DIMANA jenis = 'Tenera' AND umur > 5")

	// Create index
	fmt.Println("[7] Creating index on jenis field...")
	res, err = c.Query("INDEKS sawit_block_a PADA jenis", nil)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Result: %v\n", res)
	}
	fmt.Println("")

	// Show indexes
	fmt.Println("[8] Showing indexes...")
	printQuery(c, "LIHAT INDEKS sawit_block_a")

	// Aggregate - COUNT
	fmt.Println("[9] Aggregation - COUNT...")
	printQuery(c, "HITUNG COUNT(*) DARI sawit_block_a")

	// Aggregate - AVG
	fmt.Println("[10] Aggregation - AVG(produksi)...")
	printQuery(c, "HITUNG AVG(produksi) DARI sawit_block_a")

	// Aggregate - GROUP BY
	fmt.Println("[11] Aggregation - COUNT by jenis (GROUP BY)...")
	printQuery(c, "HITUNG COUNT(*) DARI sawit_block_a KELOMPOK jenis")

	// Update
	fmt.Println("[12] Updating data...")
	res, err = c.Query("PUPUK sawit_block_a DENGAN produksi=250 DIMANA id = 5", nil)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Result: %v\n", res)
	}
	fmt.Println("")

	// Verify update
	fmt.Println("[13] Verifying update...")
	printQuery(c, "PANEN * DARI sawit_block_a DIMANA id = 5")

	// Delete
	fmt.Println("[14] Deleting data...")
	res, err = c.Query("GUSUR DARI sawit_block_a DIMANA id = 4", nil)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Result: %v\n", res)
	}
	fmt.Println("")

	// Final count
	fmt.Println("[15] Final count...")
	printQuery(c, "HITUNG COUNT(*) DARI sawit_block_a")

	// List all databases
	fmt.Println("[16] Listing all databases...")
	dbs, err := c.ListDatabases()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Databases: %v\n", dbs)
	}
	fmt.Println("")

	// Ping test
	fmt.Println("[17] Ping test...")
	latency, err := c.Ping()
	if err != nil {
		log.Printf("Ping error: %v", err)
	} else {
		fmt.Printf("Latency: %dms\n", latency)
	}
	fmt.Println("")

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║   ✓ All operations completed successfully!      ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
}

func printQuery(c *client.SawitClient, q string) {
	res, err := c.Query(q, nil)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Printf("Result: %s\n\n", string(b))
}
