package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/WowoEngine/SawitDB-Go/internal/engine"
)

type Config struct {
	Port           int
	Host           string
	DataDir        string
	MaxConnections int
	QueryTimeout   time.Duration
	LogLevel       string
	Auth           map[string]string
}

type SawitServer struct {
	Config    Config
	Databases map[string]*engine.SawitDB
	Clients   sync.Map // net.Conn -> bool
	Listener  net.Listener
	Stats     ServerStats
	Mu        sync.Mutex
}

type ServerStats struct {
	TotalConnections  int
	ActiveConnections int
	TotalQueries      int
	Errors            int
	StartTime         time.Time
}

func NewSawitServer(config Config) *SawitServer {
	if config.DataDir == "" {
		config.DataDir = "../data"
	}
	if _, err := os.Stat(config.DataDir); os.IsNotExist(err) {
		os.MkdirAll(config.DataDir, os.ModePerm)
	}
	return &SawitServer{
		Config:    config,
		Databases: make(map[string]*engine.SawitDB),
		Stats: ServerStats{
			StartTime: time.Now(),
		},
	}
}

func (s *SawitServer) Start() {
	addr := fmt.Sprintf("%s:%d", s.Config.Host, s.Config.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("[Server] Error starting: %v\n", err)
		return
	}
	s.Listener = ln

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║         🌴 SawitDB Server - Version 1.0 (Go)     ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Printf("[Server] Listening on %s\n", addr)
	fmt.Printf("[Server] Protocol: sawitdb://%s/[database]\n", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("[Server] Accept error: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *SawitServer) log(level string, message string) {
	// Simple logging
	fmt.Printf("[%s] [%s] %s\n", time.Now().Format(time.RFC3339), strings.ToUpper(level), message)
}

func (s *SawitServer) handleConnection(conn net.Conn) {
	clientId := conn.RemoteAddr().String()

	s.Mu.Lock()
	if s.Stats.ActiveConnections >= s.Config.MaxConnections {
		s.Mu.Unlock()
		s.log("warn", "Connection limit reached. Rejecting "+clientId)
		s.sendError(conn, "Server connection limit reached")
		conn.Close()
		return
	}
	s.Stats.TotalConnections++
	s.Stats.ActiveConnections++
	s.Mu.Unlock()

	s.Clients.Store(conn, true)
	s.log("info", "Client connected: "+clientId)

	defer func() {
		s.log("info", "Client disconnected: "+clientId)
		conn.Close()
		s.Clients.Delete(conn)
		s.Mu.Lock()
		s.Stats.ActiveConnections--
		s.Mu.Unlock()
	}()

	// Send welcome
	s.sendResponse(conn, map[string]interface{}{
		"type":     "welcome",
		"message":  "SawitDB Server (Go)",
		"version":  "1.0",
		"protocol": "sawitdb",
	})

	reader := bufio.NewReader(conn)
	authenticated := s.Config.Auth == nil
	var currentDatabase string

	for {
		// Expect JSON lines
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				s.log("error", "Read error: "+err.Error())
			}
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var req map[string]interface{}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			s.log("error", "Invalid JSON: "+err.Error())
			s.sendError(conn, "Invalid request format: "+err.Error())
			continue
		}

		s.handleRequest(conn, req, &authenticated, &currentDatabase)
	}
}

func (s *SawitServer) handleRequest(conn net.Conn, req map[string]interface{}, authenticated *bool, currentDatabase *string) {
	reqType, _ := req["type"].(string)
	payload, _ := req["payload"].(map[string]interface{})

	if s.Config.Auth != nil && !*authenticated && reqType != "auth" {
		s.sendError(conn, "Authentication required")
		return
	}

	switch reqType {
	case "auth":
		user, _ := payload["username"].(string)
		pass, _ := payload["password"].(string)
		if s.Config.Auth == nil {
			*authenticated = true
			s.sendResponse(conn, map[string]interface{}{"type": "auth_success", "message": "No auth required"})
		} else if correct, ok := s.Config.Auth[user]; ok && correct == pass {
			*authenticated = true
			s.sendResponse(conn, map[string]interface{}{"type": "auth_success", "message": "Authentication successful"})
		} else {
			s.sendError(conn, "Invalid credentials")
		}

	case "use":
		dbName, _ := payload["database"].(string)
		if dbName == "" {
			s.sendError(conn, "Invalid database name")
			return
		}
		if _, err := s.getOrCreateDatabase(dbName); err != nil {
			s.sendError(conn, "Failed to use database: "+err.Error())
		} else {
			*currentDatabase = dbName
			s.sendResponse(conn, map[string]interface{}{
				"type": "use_success", "database": dbName, "message": fmt.Sprintf("Switched to database '%s'", dbName),
			})
		}

	case "query":
		query, _ := payload["query"].(string)
		params, _ := payload["params"].(map[string]interface{})
		s.handleQuery(conn, query, params, currentDatabase)

	case "ping":
		s.sendResponse(conn, map[string]interface{}{"type": "pong", "timestamp": time.Now().UnixMilli()})

	case "list_databases":
		dbs, _ := s.listDatabases()
		s.sendResponse(conn, map[string]interface{}{"type": "database_list", "databases": dbs, "count": len(dbs)})

	case "drop_database":
		dbName, _ := payload["database"].(string)
		s.handleDropDatabase(conn, dbName, currentDatabase)

	case "stats": // TODO
		s.sendResponse(conn, map[string]interface{}{"type": "stats", "stats": s.Stats})

	default:
		s.sendError(conn, "Unknown request type: "+reqType)
	}
}

func (s *SawitServer) getOrCreateDatabase(name string) (*engine.SawitDB, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if db, ok := s.Databases[name]; ok {
		return db, nil
	}

	dbPath := filepath.Join(s.Config.DataDir, name+".sawit")
	db, err := engine.NewSawitDB(dbPath)
	if err != nil {
		return nil, err
	}
	s.Databases[name] = db
	return db, nil
}

func (s *SawitServer) listDatabases() ([]string, error) {
	files, err := os.ReadDir(s.Config.DataDir)
	if err != nil {
		return nil, err
	}
	res := []string{}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sawit") {
			res = append(res, strings.TrimSuffix(f.Name(), ".sawit"))
		}
	}
	return res, nil
}

func (s *SawitServer) handleQuery(conn net.Conn, query string, params map[string]interface{}, currentDb *string) {
	startTime := time.Now()

	// Server Level Commands Intercept
	qUpper := strings.ToUpper(strings.TrimSpace(query))

	if qUpper == "LIHAT WILAYAH" || qUpper == "SHOW DATABASES" {
		dbs, _ := s.listDatabases()
		listStr := strings.Join(dbs, "\n- ")
		s.sendResponse(conn, map[string]interface{}{"type": "query_result", "result": "Daftar Wilayah:\n- " + listStr, "query": query, "executionTime": 0})
		return
	}

	if strings.HasPrefix(qUpper, "BUKA WILAYAH") || strings.HasPrefix(qUpper, "CREATE DATABASE") {
		// parsing...
		parts := strings.Fields(query)
		if len(parts) < 3 {
			s.sendError(conn, "Syntax: BUKA WILAYAH [nama]")
			return
		}
		// assuming index 2 is name (BUKA WILAYAH name) or (CREATE DATABASE name)
		name := parts[2]

		// Validation (alphanumeric check omitted for brevity but recommended)
		dbPath := filepath.Join(s.Config.DataDir, name+".sawit")
		if _, err := os.Stat(dbPath); err == nil {
			s.sendResponse(conn, map[string]interface{}{"type": "query_result", "result": fmt.Sprintf("Wilayah '%s' sudah ada.", name)})
			return
		}

		if _, err := s.getOrCreateDatabase(name); err != nil {
			s.sendError(conn, err.Error())
		} else {
			s.sendResponse(conn, map[string]interface{}{"type": "query_result", "result": fmt.Sprintf("Wilayah '%s' berhasil dibuka.", name)})
		}
		return
	}

	if strings.HasPrefix(qUpper, "MASUK WILAYAH") || strings.HasPrefix(qUpper, "USE") {
		parts := strings.Fields(query)
		name := ""
		if strings.HasPrefix(qUpper, "USE") {
			if len(parts) < 2 {
				s.sendError(conn, "Syntax: USE [name]")
				return
			}
			name = parts[1]
		} else {
			if len(parts) < 3 {
				s.sendError(conn, "Syntax: MASUK WILAYAH [nama]")
				return
			}
			name = parts[2]
		}

		path := filepath.Join(s.Config.DataDir, name+".sawit")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			s.sendError(conn, fmt.Sprintf("Wilayah '%s' tidak ditemukan.", name))
			return
		}
		*currentDb = name
		s.sendResponse(conn, map[string]interface{}{"type": "query_result", "result": fmt.Sprintf("Selamat datang di wilayah '%s'.", name)})
		return
	}

	if strings.HasPrefix(qUpper, "BAKAR WILAYAH") || strings.HasPrefix(qUpper, "DROP DATABASE") {
		parts := strings.Fields(query)
		if len(parts) < 3 {
			s.sendError(conn, "Syntax: BAKAR WILAYAH [nama]")
			return
		}
		name := parts[2]

		path := filepath.Join(s.Config.DataDir, name+".sawit")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			s.sendError(conn, fmt.Sprintf("Wilayah '%s' tidak ditemukan.", name))
			return
		}

		s.Mu.Lock()
		delete(s.Databases, name)
		s.Mu.Unlock()

		os.Remove(path)

		if *currentDb == name {
			*currentDb = ""
		}

		s.sendResponse(conn, map[string]interface{}{"type": "query_result", "result": fmt.Sprintf("Wilayah '%s' telah hangus terbakar.", name)})
		return
	}

	if *currentDb == "" {
		s.sendError(conn, "Anda belum masuk wilayah manapun. Gunakan: MASUK WILAYAH [nama]")
		return
	}

	db, err := s.getOrCreateDatabase(*currentDb)
	if err != nil {
		s.sendError(conn, err.Error())
		return
	}

	res, err := db.Query(query, params)
	duration := time.Since(startTime).Milliseconds()

	if err != nil {
		s.Stats.Errors++
		s.sendError(conn, "Query error: "+err.Error())
	} else {
		s.Stats.TotalQueries++
		s.sendResponse(conn, map[string]interface{}{
			"type":          "query_result",
			"result":        res,
			"query":         query,
			"executionTime": duration,
		})
	}
}

func (s *SawitServer) handleDropDatabase(conn net.Conn, dbName string, currentDb *string) {
	if dbName == "" {
		s.sendError(conn, "Database name required")
		return
	}

	path := filepath.Join(s.Config.DataDir, dbName+".sawit")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		s.sendError(conn, "Database doesn't exist")
		return
	}

	s.Mu.Lock()
	delete(s.Databases, dbName)
	s.Mu.Unlock()

	os.Remove(path)

	if *currentDb == dbName {
		*currentDb = ""
	}

	s.sendResponse(conn, map[string]interface{}{"type": "drop_success", "database": dbName, "message": "Burned"})
}

func (s *SawitServer) sendResponse(conn net.Conn, data map[string]interface{}) {
	bytes, _ := json.Marshal(data)
	conn.Write(append(bytes, '\n'))
}

func (s *SawitServer) sendError(conn net.Conn, msg string) {
	s.sendResponse(conn, map[string]interface{}{"type": "error", "error": msg})
}

// MAIN FUNCTION
func main() {
	port := 7878
	// Parsing flags or env logic here

	server := NewSawitServer(Config{
		Port:           port,
		Host:           "0.0.0.0",
		MaxConnections: 100,
		LogLevel:       "info",
	})
	server.Start()
}
