package engine

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sawitdb/internal/index"
	"sawitdb/internal/parser"
	"sawitdb/internal/storage"
	"sort"
	"strings"
)

type SawitDB struct {
	Pager   *storage.Pager
	Indexes map[string]*index.BTreeIndex
	Parser  *parser.QueryParser
}

func NewSawitDB(filePath string) (*SawitDB, error) {
	pager, err := storage.NewPager(filePath)
	if err != nil {
		return nil, err
	}
	return &SawitDB{
		Pager:   pager,
		Indexes: make(map[string]*index.BTreeIndex),
		Parser:  parser.NewQueryParser(),
	}, nil
}

func (db *SawitDB) Close() error {
	return db.Pager.Close()
}

func (db *SawitDB) Query(queryString string, params map[string]interface{}) (interface{}, error) {
	cmd := db.Parser.Parse(queryString, params)

	if cmd.Type == "EMPTY" {
		return "", nil
	}
	if cmd.Type == "ERROR" {
		return nil, errors.New(cmd.Message)
	}

	defer func() {
		if r := recover(); r != nil {
			// Catch panic
		}
	}()

	switch cmd.Type {
	case "CREATE_TABLE":
		return db.createTable(cmd.Table)
	case "SHOW_TABLES":
		return db.showTables()
	case "SHOW_INDEXES":
		return db.showIndexes(cmd.Table)
	case "INSERT":
		return db.insert(cmd.Table, cmd.Data)
	case "SELECT":
		rows, err := db._select(cmd.Table, cmd.Criteria, cmd.Sort, cmd.Limit, cmd.Offset)
		if err != nil {
			return nil, err
		}

		if len(cmd.Cols) == 1 && cmd.Cols[0] == "*" { // Logic to handle * or empty
			// Return all
			return rows, nil
		}
		// In JS impl: checks if cols[0] == '*'.
		// My parser returns cols list. If it was empty/star, it might be handled.
		// If cols is empty, it means *
		if len(cmd.Cols) == 0 || (len(cmd.Cols) == 1 && cmd.Cols[0] == "*") {
			return rows, nil
		}

		// Projection
		projected := make([]map[string]interface{}, len(rows))
		for i, r := range rows {
			newRow := make(map[string]interface{})
			for _, c := range cmd.Cols {
				if v, ok := r[c]; ok {
					newRow[c] = v
				}
			}
			projected[i] = newRow
		}
		return projected, nil

	case "DELETE":
		return db.delete(cmd.Table, cmd.Criteria)
	case "UPDATE":
		return db.update(cmd.Table, cmd.Updates, cmd.Criteria)
	case "DROP_TABLE":
		return db.dropTable(cmd.Table)
	case "CREATE_INDEX":
		return db.createIndex(cmd.Table, cmd.Field)
	case "AGGREGATE":
		return db.aggregate(cmd.Table, cmd.Func, cmd.Field, cmd.Criteria, cmd.GroupBy)
	default:
		return nil, errors.New("Perintah tidak dikenal atau belum diimplementasikan")
	}
}

// --- Core Logic ---

type TableEntry struct {
	Index     int
	Offset    int64
	StartPage uint32
	LastPage  uint32
}

func (db *SawitDB) findTableEntry(name string) (*TableEntry, error) {
	p0, err := db.Pager.ReadPage(0)
	if err != nil {
		return nil, err
	}

	numTables := binary.LittleEndian.Uint32(p0[8:])
	offset := 12

	for i := 0; i < int(numTables); i++ {
		// Name is 32 bytes
		tNameBytes := p0[offset : offset+32]
		// Remove nulls
		tName := strings.TrimRight(string(tNameBytes), "\x00")
		if tName == name {
			return &TableEntry{
				Index:     i,
				Offset:    int64(offset),
				StartPage: binary.LittleEndian.Uint32(p0[offset+32:]),
				LastPage:  binary.LittleEndian.Uint32(p0[offset+36:]),
			}, nil
		}
		offset += 40
	}
	return nil, nil
}

func (db *SawitDB) showTables() ([]string, error) {
	p0, err := db.Pager.ReadPage(0)
	if err != nil {
		return nil, err
	}
	numTables := binary.LittleEndian.Uint32(p0[8:])
	tables := []string{}
	offset := 12
	for i := 0; i < int(numTables); i++ {
		tName := strings.TrimRight(string(p0[offset:offset+32]), "\x00")
		tables = append(tables, tName)
		offset += 40
	}
	return tables, nil
}

func (db *SawitDB) createTable(name string) (string, error) {
	if name == "" {
		return "", errors.New("Nama kebun tidak boleh kosong")
	}
	if len(name) > 32 {
		return "", errors.New("Nama kebun max 32 karakter")
	}

	entry, err := db.findTableEntry(name)
	if err != nil {
		return "", err
	}
	if entry != nil {
		return fmt.Sprintf("Kebun '%s' sudah ada.", name), nil
	}

	p0, err := db.Pager.ReadPage(0)
	if err != nil {
		return "", err
	}
	numTables := binary.LittleEndian.Uint32(p0[8:])

	offset := 12 + (int(numTables) * 40)
	if offset+40 > storage.PAGE_SIZE {
		return "", errors.New("Lahan penuh (Page 0 full)")
	}

	newPageId, err := db.Pager.AllocPage()
	if err != nil {
		return "", err
	}

	// Write name
	copy(p0[offset:], name)
	// Write Pages
	binary.LittleEndian.PutUint32(p0[offset+32:], newPageId)
	binary.LittleEndian.PutUint32(p0[offset+36:], newPageId)

	// Update count
	binary.LittleEndian.PutUint32(p0[8:], numTables+1)

	err = db.Pager.WritePage(0, p0)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Kebun '%s' telah dibuka.", name), nil
}

func (db *SawitDB) dropTable(name string) (string, error) {
	entry, err := db.findTableEntry(name)
	if err != nil {
		return "", err
	}
	if entry == nil {
		return fmt.Sprintf("Kebun '%s' tidak ditemukan.", name), nil
	}

	p0, err := db.Pager.ReadPage(0)
	if err != nil {
		return "", err
	}
	numTables := binary.LittleEndian.Uint32(p0[8:])

	// Move last entry to fill gap (if needed)
	if int(numTables) > 1 && entry.Index < int(numTables)-1 {
		lastOffset := 12 + ((int(numTables) - 1) * 40)
		copy(p0[entry.Offset:entry.Offset+40], p0[lastOffset:lastOffset+40])
	}

	// Clear last spot (optional but good)
	lastOffset := 12 + ((int(numTables) - 1) * 40)
	for k := 0; k < 40; k++ {
		p0[lastOffset+k] = 0
	}

	binary.LittleEndian.PutUint32(p0[8:], numTables-1)
	err = db.Pager.WritePage(0, p0)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Kebun '%s' telah dibakar (Drop).", name), nil
}

func (db *SawitDB) updateTableLastPage(name string, newLastPageId uint32) error {
	entry, err := db.findTableEntry(name)
	if err != nil {
		return err
	}
	if entry == nil {
		return errors.New("Internal Error: Table missing for update")
	}

	p0, err := db.Pager.ReadPage(0)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(p0[entry.Offset+36:], newLastPageId)
	return db.Pager.WritePage(0, p0)
}

func (db *SawitDB) insert(table string, data map[string]interface{}) (string, error) {
	if len(data) == 0 {
		return "", errors.New("Data kosong")
	}

	entry, err := db.findTableEntry(table)
	if err != nil {
		return "", err
	}
	if entry == nil {
		return "", fmt.Errorf("Kebun '%s' tidak ditemukan.", table)
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	recordLen := len(dataBytes)
	totalLen := 2 + recordLen

	currentPageId := entry.LastPage
	pData, err := db.Pager.ReadPage(currentPageId)
	if err != nil {
		return "", err
	}

	freeOffset := binary.LittleEndian.Uint16(pData[6:])

	if int(freeOffset)+totalLen > storage.PAGE_SIZE {
		newPageId, err := db.Pager.AllocPage()
		if err != nil {
			return "", err
		}

		// Set Next Page of current
		binary.LittleEndian.PutUint32(pData[0:], newPageId)
		db.Pager.WritePage(currentPageId, pData)

		currentPageId = newPageId
		pData, err = db.Pager.ReadPage(currentPageId)
		if err != nil {
			return "", err
		}
		freeOffset = binary.LittleEndian.Uint16(pData[6:])

		db.updateTableLastPage(table, currentPageId)
	}

	binary.LittleEndian.PutUint16(pData[freeOffset:], uint16(recordLen))
	copy(pData[freeOffset+2:], dataBytes)

	count := binary.LittleEndian.Uint16(pData[4:])
	binary.LittleEndian.PutUint16(pData[4:], count+1)
	binary.LittleEndian.PutUint16(pData[6:], freeOffset+uint16(totalLen))

	err = db.Pager.WritePage(currentPageId, pData)
	if err != nil {
		return "", err
	}

	// Indexes
	db.updateIndexes(table, data)

	return "Bibit tertanam.", nil
}

func (db *SawitDB) updateIndexes(table string, data map[string]interface{}) {
	for indexKey, index := range db.Indexes {
		parts := strings.Split(indexKey, ".")
		if parts[0] == table {
			field := parts[1]
			if val, ok := data[field]; ok {
				index.Insert(val, data)
			}
		}
	}
}

func (db *SawitDB) checkMatch(obj map[string]interface{}, criteria *parser.Criteria) bool {
	if criteria == nil {
		return true
	}

	if criteria.Type == "compound" {
		result := true
		for i, cond := range criteria.Conditions {
			matches := db.checkSingleCondition(obj, cond)
			if i == 0 {
				result = matches
			} else {
				if cond.Logic == "OR" {
					result = result || matches
				} else {
					result = result && matches
				}
			}
		}
		return result
	}
	return db.checkSingleCondition(obj, criteria)
}

func (db *SawitDB) checkSingleCondition(obj map[string]interface{}, criteria *parser.Criteria) bool {
	val, exists := obj[criteria.Key]
	target := criteria.Val
	op := criteria.Op

	// If key doesn't exist? In JS obj[key] is undefined.
	// IS NULL checks might match?
	if !exists {
		if op == "IS NULL" {
			return true
		}
		if op == "IS NOT NULL" {
			return false
		}
		return false // Assume false?
	}

	// Helper for comparison using float64 as common denominator for numbers
	compare := func(a, b interface{}) int {
		// Attempt float conversion
		af, aOk := toFloat(a)
		bf, bOk := toFloat(b)
		if aOk && bOk {
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
		// String comparison
		as := fmt.Sprintf("%v", a)
		bs := fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}

	switch op {
	case "=":
		return compare(val, target) == 0
	case "!=":
		return compare(val, target) != 0
	case ">":
		return compare(val, target) > 0
	case "<":
		return compare(val, target) < 0
	case ">=":
		return compare(val, target) >= 0
	case "<=":
		return compare(val, target) <= 0
	case "IN":
		if arr, ok := target.([]interface{}); ok {
			for _, v := range arr {
				if compare(val, v) == 0 {
					return true
				}
			}
		}
		return false
	case "NOT IN":
		if arr, ok := target.([]interface{}); ok {
			for _, v := range arr {
				if compare(val, v) == 0 {
					return false
				}
			}
		}
		return true
	case "LIKE":
		// Regex
		sTarget := fmt.Sprintf("%v", target)
		sVal := fmt.Sprintf("%v", val)
		regexStr := "^" + strings.ReplaceAll(sTarget, "%", ".*") + "$"
		valid, _ := regexp.MatchString("(?i)"+regexStr, sVal)
		return valid
	case "BETWEEN":
		if arr, ok := target.([]interface{}); ok && len(arr) == 2 {
			return compare(val, arr[0]) >= 0 && compare(val, arr[1]) <= 0
		}
		return false
	case "IS NULL":
		return val == nil
	case "IS NOT NULL":
		return val != nil
	}
	return false
}

func toFloat(i interface{}) (float64, bool) {
	switch v := i.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	default:
		return 0, false
	}
}

func (db *SawitDB) _select(table string, criteria *parser.Criteria, sortOpt *parser.Sort, limit, offset *int) ([]map[string]interface{}, error) {
	entry, err := db.findTableEntry(table)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("Kebun '%s' tidak ditemukan.", table)
	}

	var results []map[string]interface{}

	// Optimization: If Index exists and criteria is simple '=' and no sort
	useIndex := false
	if criteria != nil && criteria.Type == "" && criteria.Op == "=" && sortOpt == nil {
		indexKey := fmt.Sprintf("%s.%s", table, criteria.Key)
		if idx, ok := db.Indexes[indexKey]; ok {
			// Found index
			useIndex = true
			results = []map[string]interface{}{}
			// Search returns []interface{}. We expect they are map[string]interface{} (the rows)
			// In Insert, we perform: index.Insert(val, data). data IS the row map.
			matched := idx.Search(criteria.Val)
			for _, m := range matched {
				if r, ok := m.(map[string]interface{}); ok {
					results = append(results, r)
				}
			}
		}
	}

	if !useIndex {
		// Full scan
		results, err = db.scanTable(entry, criteria)
		if err != nil {
			return nil, err
		}
	}

	// Sort
	if sortOpt != nil {
		sort.Slice(results, func(i, j int) bool {
			valA := results[i][sortOpt.Key]
			valB := results[j][sortOpt.Key]
			// Use comparison helper
			// copy paste compare
			af, aOk := toFloat(valA)
			bf, bOk := toFloat(valB)
			less := false
			if aOk && bOk {
				less = af < bf
			} else {
				less = fmt.Sprintf("%v", valA) < fmt.Sprintf("%v", valB)
			}

			if sortOpt.Dir == "asc" {
				return less
			}
			// If desc, return greater (or !less if equal handling matters, but simple reversal)
			if aOk && bOk {
				return af > bf
			}
			return fmt.Sprintf("%v", valA) > fmt.Sprintf("%v", valB)
		})
	}

	// Limit & Offset
	startIndex := 0
	endIndex := len(results)

	if offset != nil {
		startIndex = *offset
	}
	if limit != nil {
		endIndex = startIndex + *limit
	}

	if startIndex >= len(results) {
		return []map[string]interface{}{}, nil
	}
	if endIndex > len(results) {
		endIndex = len(results)
	}

	return results[startIndex:endIndex], nil
}

func (db *SawitDB) scanTable(entry *TableEntry, criteria *parser.Criteria) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	currentPageId := entry.StartPage

	for currentPageId != 0 {
		pData, err := db.Pager.ReadPage(currentPageId)
		if err != nil {
			return nil, err
		}

		count := binary.LittleEndian.Uint16(pData[4:])
		offset := 8

		for i := 0; i < int(count); i++ {
			recLen := binary.LittleEndian.Uint16(pData[offset:])
			jsonBytes := pData[offset+2 : offset+2+int(recLen)]

			var obj map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &obj); err == nil {
				if db.checkMatch(obj, criteria) {
					results = append(results, obj)
				}
			}

			offset += 2 + int(recLen)
		}
		currentPageId = binary.LittleEndian.Uint32(pData[0:])
	}
	return results, nil
}

func (db *SawitDB) delete(table string, criteria *parser.Criteria) (string, error) {
	entry, err := db.findTableEntry(table)
	if err != nil {
		return "", err
	}
	if entry == nil {
		return "", fmt.Errorf("Kebun '%s' tidak ditemukan.", table)
	}

	currentPageId := entry.StartPage
	deletedCount := 0

	for currentPageId != 0 {
		pData, err := db.Pager.ReadPage(currentPageId)
		if err != nil {
			return "", err
		}

		count := binary.LittleEndian.Uint16(pData[4:])
		offset := 8

		type RecordCtx struct {
			Len  int
			Data []byte
		}
		recordsToKeep := []RecordCtx{}

		for i := 0; i < int(count); i++ {
			recLen := int(binary.LittleEndian.Uint16(pData[offset:]))
			jsonBytes := pData[offset+2 : offset+2+recLen]

			shouldDelete := false
			var obj map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &obj); err == nil {
				if db.checkMatch(obj, criteria) {
					shouldDelete = true
				}
			}

			if shouldDelete {
				deletedCount++
			} else {
				recordsToKeep = append(recordsToKeep, RecordCtx{
					Len:  recLen,
					Data: pData[offset+2 : offset+2+recLen], // Safe slice? Copy if needed
				})
			}
			offset += 2 + recLen
		}

		if len(recordsToKeep) < int(count) {
			writeOffset := 8
			binary.LittleEndian.PutUint16(pData[4:], uint16(len(recordsToKeep)))
			for _, rec := range recordsToKeep {
				binary.LittleEndian.PutUint16(pData[writeOffset:], uint16(rec.Len))
				copy(pData[writeOffset+2:], rec.Data)
				writeOffset += 2 + rec.Len
			}
			binary.LittleEndian.PutUint16(pData[6:], uint16(writeOffset))

			// Zero out rest
			for k := writeOffset; k < storage.PAGE_SIZE; k++ {
				pData[k] = 0
			}

			db.Pager.WritePage(currentPageId, pData)
		}
		currentPageId = binary.LittleEndian.Uint32(pData[0:])
	}

	return fmt.Sprintf("Berhasil menggusur %d bibit.", deletedCount), nil
}

func (db *SawitDB) update(table string, updates map[string]interface{}, criteria *parser.Criteria) (string, error) {
	records, err := db._select(table, criteria, nil, nil, nil)
	if err != nil {
		return "", err
	}
	if len(records) == 0 {
		return "Tidak ada bibit yang cocok untuk dipupuk.", nil
	}

	// Inefficient: Delete then Insert
	db.delete(table, criteria)

	count := 0
	for _, rec := range records {
		for k, v := range updates {
			rec[k] = v
		}
		db.insert(table, rec)
		count++
	}
	return fmt.Sprintf("Berhasil memupuk %d bibit.", count), nil
}

func (db *SawitDB) createIndex(table string, field string) (string, error) {
	entry, err := db.findTableEntry(table)
	if err != nil {
		return "", err
	}
	if entry == nil {
		return "", fmt.Errorf("Kebun '%s' tidak ditemukan.", table)
	}

	indexKey := fmt.Sprintf("%s.%s", table, field)
	if _, ok := db.Indexes[indexKey]; ok {
		return fmt.Sprintf("Indeks pada '%s' sudah ada.", indexKey), nil
	}

	index := index.NewBTreeIndex(32)
	index.Name = indexKey
	index.KeyField = field

	// Build
	records, _ := db._select(table, nil, nil, nil, nil) // All
	for _, rec := range records {
		if val, ok := rec[field]; ok {
			index.Insert(val, rec)
		}
	}

	db.Indexes[indexKey] = index
	return fmt.Sprintf("Indeks dibuat pada '%s' (%d records indexed)", indexKey, len(records)), nil
}

func (db *SawitDB) showIndexes(table string) (interface{}, error) {
	if table != "" {
		res := []interface{}{}
		for key, idx := range db.Indexes {
			if strings.HasPrefix(key, table+".") {
				res = append(res, idx.Stats())
			}
		}
		if len(res) > 0 {
			return res, nil
		}
		return fmt.Sprintf("Tidak ada indeks pada '%s'", table), nil
	}
	res := []interface{}{}
	for _, idx := range db.Indexes {
		res = append(res, idx.Stats())
	}
	return res, nil
}

func (db *SawitDB) aggregate(table string, fn string, field string, criteria *parser.Criteria, groupBy string) (interface{}, error) {
	records, err := db._select(table, criteria, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	if groupBy != "" {
		return db.groupedAggregate(records, fn, field, groupBy), nil
	}

	// Simple aggregate
	switch strings.ToUpper(fn) {
	case "COUNT":
		return map[string]int{"count": len(records)}, nil
	case "SUM":
		sum := 0.0
		for _, r := range records {
			if v, ok := toFloat(r[field]); ok {
				sum += v
			}
		}
		return map[string]interface{}{"sum": sum, "field": field}, nil
	case "AVG":
		if len(records) == 0 {
			return map[string]interface{}{"avg": 0, "count": 0}, nil
		}
		sum := 0.0
		for _, r := range records {
			if v, ok := toFloat(r[field]); ok {
				sum += v
			}
		}
		return map[string]interface{}{"avg": sum / float64(len(records)), "field": field, "count": len(records)}, nil
	case "MIN":
		minVal := math.Inf(1)
		for _, r := range records {
			if v, ok := toFloat(r[field]); ok {
				if v < minVal {
					minVal = v
				}
			}
		}
		if math.IsInf(minVal, 1) {
			minVal = 0
		}
		return map[string]interface{}{"min": minVal, "field": field}, nil
	case "MAX":
		maxVal := math.Inf(-1)
		for _, r := range records {
			if v, ok := toFloat(r[field]); ok {
				if v > maxVal {
					maxVal = v
				}
			}
		}
		if math.IsInf(maxVal, -1) {
			maxVal = 0
		}
		return map[string]interface{}{"max": maxVal, "field": field}, nil
	}
	return nil, errors.New("Unknown aggregate function")
}

func (db *SawitDB) groupedAggregate(records []map[string]interface{}, fn, field, groupBy string) interface{} {
	groups := make(map[interface{}][]map[string]interface{})

	for _, r := range records {
		key := r[groupBy] // Can be nil or any type
		// Map key must be comparable. interface{} is comparable if underlying type is.
		// JSON numbers are float64. Strings are string.
		// Slices/maps are not comparable. Assuming groupBy is scalar.
		groups[key] = append(groups[key], r)
	}

	results := []map[string]interface{}{}
	for key, group := range groups {
		res := map[string]interface{}{groupBy: key}

		switch strings.ToUpper(fn) {
		case "COUNT":
			res["count"] = len(group)
		case "SUM":
			sum := 0.0
			for _, r := range group {
				if v, ok := toFloat(r[field]); ok {
					sum += v
				}
			}
			res["sum"] = sum
		case "AVG":
			sum := 0.0
			for _, r := range group {
				if v, ok := toFloat(r[field]); ok {
					sum += v
				}
			}
			res["avg"] = 0.0
			if len(group) > 0 {
				res["avg"] = sum / float64(len(group))
			}
		case "MIN":
			minVal := math.Inf(1)
			for _, r := range group {
				if v, ok := toFloat(r[field]); ok {
					if v < minVal {
						minVal = v
					}
				}
			}
			res["min"] = minVal
		case "MAX":
			maxVal := math.Inf(-1)
			for _, r := range group {
				if v, ok := toFloat(r[field]); ok {
					if v > maxVal {
						maxVal = v
					}
				}
			}
			res["max"] = maxVal
		}
		results = append(results, res)
	}
	return results
}
