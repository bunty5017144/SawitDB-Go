package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type QueryParser struct{}

func NewQueryParser() *QueryParser {
	return &QueryParser{}
}

// Command represents the parsed command
type Command struct {
	Type     string
	Table    string
	Data     map[string]interface{}
	Criteria *Criteria
	Sort     *Sort
	Limit    *int
	Offset   *int
	Cols     []string
	Func     string
	Field    string
	Updates  map[string]interface{}
	GroupBy  string
	Message  string // For ERROR
}

type Sort struct {
	Key string
	Dir string
}

type Criteria struct {
	Type       string // "compound" or ""
	Conditions []*Criteria
	Key        string
	Op         string
	Val        interface{}
	Logic      string // AND / OR
}

func (qp *QueryParser) Tokenize(sql string) []string {
	// \s*(=>|!=|>=|<=|<>|[(),=*.<>?]|[a-zA-Z_]\w*|@\w+|\d+|'[^']*'|"[^"]*")\s*
	re := regexp.MustCompile(`\s*(=>|!=|>=|<=|<>|[(),=*.<>?]|[a-zA-Z_]\w*|@\w+|\d+|'[^']*'|"[^"]*")\s*`)
	matches := re.FindAllStringSubmatch(sql, -1)
	tokens := make([]string, 0, len(matches))
	for _, m := range matches {
		tokens = append(tokens, m[1])
	}
	return tokens
}

func (qp *QueryParser) Parse(queryString string, params map[string]interface{}) *Command {
	tokens := qp.Tokenize(queryString)
	if len(tokens) == 0 {
		return &Command{Type: "EMPTY"}
	}

	cmd := strings.ToUpper(tokens[0])
	var command *Command
	var err error

	defer func() {
		if r := recover(); r != nil {
			command = &Command{Type: "ERROR", Message: fmt.Sprintf("%v", r)}
		}
	}()

	switch cmd {
	case "LAHAN", "CREATE":
		if len(tokens) > 1 && strings.ToUpper(tokens[1]) == "INDEX" {
			command, err = qp.parseCreateIndex(tokens)
		} else {
			command, err = qp.parseCreate(tokens)
		}
	case "LIHAT", "SHOW":
		command, err = qp.parseShow(tokens)
	case "TANAM", "INSERT":
		command, err = qp.parseInsert(tokens)
	case "PANEN", "SELECT":
		command, err = qp.parseSelect(tokens)
	case "GUSUR", "DELETE":
		command, err = qp.parseDelete(tokens)
	case "PUPUK", "UPDATE":
		command, err = qp.parseUpdate(tokens)
	case "BAKAR", "DROP":
		command, err = qp.parseDrop(tokens)
	case "INDEKS":
		command, err = qp.parseCreateIndex(tokens)
	case "HITUNG":
		command, err = qp.parseAggregate(tokens)
	default:
		return &Command{Type: "ERROR", Message: fmt.Sprintf("Perintah tidak dikenal: %s", cmd)}
	}

	if err != nil {
		return &Command{Type: "ERROR", Message: err.Error()}
	}

	if params != nil {
		qp.bindParameters(command, params)
	}
	return command
}

// Helpers
func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func (qp *QueryParser) parseCreate(tokens []string) (*Command, error) {
	var name string
	if strings.ToUpper(tokens[0]) == "CREATE" {
		if strings.ToUpper(tokens[1]) != "TABLE" {
			return nil, errors.New("Syntax: CREATE TABLE [name]")
		}
		name = tokens[2]
	} else {
		if len(tokens) < 2 {
			return nil, errors.New("Syntax: LAHAN [nama_kebun]")
		}
		name = tokens[1]
	}
	return &Command{Type: "CREATE_TABLE", Table: name}, nil
}

func (qp *QueryParser) parseShow(tokens []string) (*Command, error) {
	cmd := strings.ToUpper(tokens[0])
	sub := ""
	if len(tokens) > 1 {
		sub = strings.ToUpper(tokens[1])
	}

	if cmd == "LIHAT" {
		if sub == "LAHAN" {
			return &Command{Type: "SHOW_TABLES"}, nil
		}
		if sub == "INDEKS" {
			table := ""
			if len(tokens) > 2 {
				table = tokens[2]
			}
			return &Command{Type: "SHOW_INDEXES", Table: table}, nil
		}
	} else if cmd == "SHOW" {
		if sub == "TABLES" {
			return &Command{Type: "SHOW_TABLES"}, nil
		}
		if sub == "INDEXES" {
			table := ""
			if len(tokens) > 2 {
				table = tokens[2]
			}
			return &Command{Type: "SHOW_INDEXES", Table: table}, nil
		}
	}
	return nil, errors.New("Syntax: LIHAT LAHAN | SHOW TABLES | LIHAT INDEKS [table] | SHOW INDEXES")
}

func (qp *QueryParser) parseDrop(tokens []string) (*Command, error) {
	if strings.ToUpper(tokens[0]) == "DROP" {
		if len(tokens) > 1 && strings.ToUpper(tokens[1]) == "TABLE" {
			return &Command{Type: "DROP_TABLE", Table: tokens[2]}, nil
		}
	} else if strings.ToUpper(tokens[0]) == "BAKAR" {
		if len(tokens) > 1 && strings.ToUpper(tokens[1]) == "LAHAN" {
			return &Command{Type: "DROP_TABLE", Table: tokens[2]}, nil
		}
	}
	return nil, errors.New("Syntax: BAKAR LAHAN [nama] | DROP TABLE [nama]")
}

func (qp *QueryParser) parseInsert(tokens []string) (*Command, error) {
	i := 1
	var table string

	if strings.ToUpper(tokens[0]) == "INSERT" {
		if strings.ToUpper(tokens[1]) != "INTO" {
			return nil, errors.New("Syntax: INSERT INTO [table] ...")
		}
		i = 2
	} else {
		if strings.ToUpper(tokens[1]) != "KE" {
			return nil, errors.New("Syntax: TANAM KE [kebun] ...")
		}
		i = 2
	}

	table = tokens[i]
	i++

	cols := []string{}
	if tokens[i] == "(" {
		i++
		for tokens[i] != ")" {
			if tokens[i] != "," {
				cols = append(cols, tokens[i])
			}
			i++
			if i >= len(tokens) {
				return nil, errors.New("Unclosed parenthesis in columns")
			}
		}
		i++
	} else {
		return nil, errors.New("Syntax: ... [table] (col1, ...) ...")
	}

	valueKeyword := strings.ToUpper(tokens[i])
	if valueKeyword != "BIBIT" && valueKeyword != "VALUES" {
		return nil, errors.New("Expected BIBIT or VALUES")
	}
	i++

	vals := []interface{}{}
	if tokens[i] == "(" {
		i++
		for tokens[i] != ")" {
			if tokens[i] != "," {
				valStr := tokens[i]
				var val interface{} = valStr
				if strings.HasPrefix(valStr, "'") || strings.HasPrefix(valStr, "\"") {
					val = valStr[1 : len(valStr)-1]
				} else if strings.ToUpper(valStr) == "NULL" {
					val = nil
				} else if strings.ToUpper(valStr) == "TRUE" {
					val = true
				} else if strings.ToUpper(valStr) == "FALSE" {
					val = false
				} else {
					if f, err := strconv.ParseFloat(valStr, 64); err == nil {
						val = f
					}
				}
				vals = append(vals, val)
			}
			i++
		}
	} else {
		return nil, errors.New("Syntax: ... VALUES (val1, ...)")
	}

	if len(cols) != len(vals) {
		return nil, errors.New("Columns and Values count mismatch")
	}

	data := make(map[string]interface{})
	for k := 0; k < len(cols); k++ {
		data[cols[k]] = vals[k]
	}

	return &Command{Type: "INSERT", Table: table, Data: data}, nil
}

func (qp *QueryParser) parseSelect(tokens []string) (*Command, error) {
	i := 1
	cols := []string{}

	// Collect cols until FROM or DARI
	for i < len(tokens) {
		upper := strings.ToUpper(tokens[i])
		if upper == "DARI" || upper == "FROM" {
			break
		}
		if tokens[i] != "," {
			cols = append(cols, tokens[i])
		}
		i++
	}

	if i >= len(tokens) {
		return nil, errors.New("Expected DARI or FROM")
	}
	i++ // skip FROM

	table := tokens[i]
	i++

	var criteria *Criteria
	if i < len(tokens) {
		upper := strings.ToUpper(tokens[i])
		if upper == "DIMANA" || upper == "WHERE" {
			i++
			criteria = qp.parseWhere(tokens, &i)
		}
	}

	// parseWhere advances i properly?? No, JS impl scanned until keywords manually in parent.
	// We need to advance i in parseWhere or after.
	// My parseWhere implementation below will handle 'i' advancement.

	var sort *Sort
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "ORDER" {
		i++ // ORDER
		if i < len(tokens) && strings.ToUpper(tokens[i]) == "BY" {
			i++
		}
		key := tokens[i]
		i++
		dir := "asc"
		if i < len(tokens) {
			upper := strings.ToUpper(tokens[i])
			if upper == "ASC" || upper == "DESC" {
				dir = strings.ToLower(upper)
				i++
			}
		}
		sort = &Sort{Key: key, Dir: dir}
	}

	var limitVal *int
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "LIMIT" {
		i++
		val, _ := strconv.Atoi(tokens[i])
		limitVal = &val
		i++
	}

	var offsetVal *int
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "OFFSET" {
		i++
		val, _ := strconv.Atoi(tokens[i])
		offsetVal = &val
		i++
	}

	return &Command{
		Type: "SELECT", Table: table, Cols: cols, Criteria: criteria,
		Sort: sort, Limit: limitVal, Offset: offsetVal,
	}, nil
}

func (qp *QueryParser) parseWhere(tokens []string, refIndex *int) *Criteria {
	conditions := []*Criteria{}
	i := *refIndex
	currentLogic := "AND"

	for i < len(tokens) {
		token := tokens[i]
		upper := strings.ToUpper(token)

		if upper == "AND" || upper == "OR" {
			currentLogic = upper
			i++
			continue
		}

		if upper == "DENGAN" || upper == "ORDER" || upper == "LIMIT" || upper == "OFFSET" || upper == "GROUP" || upper == "KELOMPOK" {
			break
		}

		// Key Op Val
		if i < len(tokens)-1 {
			key := tokens[i]
			op := strings.ToUpper(tokens[i+1])

			// Handle BETWEEN, IS, IN etc.
			// Similar to JS logic...

			consumed := 0
			var cond *Criteria

			if op == "BETWEEN" {
				// key BETWEEN v1 AND v2
				// i=key, i+1=BETWEEN, i+2=v1, i+3=AND, i+4=v2
				v1 := normalizeVal(tokens[i+2])
				v2 := normalizeVal(tokens[i+4])
				cond = &Criteria{Key: key, Op: "BETWEEN", Val: []interface{}{v1, v2}, Logic: currentLogic}
				consumed = 5
			} else if op == "IS" {
				if strings.ToUpper(tokens[i+2]) == "NULL" {
					cond = &Criteria{Key: key, Op: "IS NULL", Logic: currentLogic}
					consumed = 3
				} else {
					// IS NOT NULL
					cond = &Criteria{Key: key, Op: "IS NOT NULL", Logic: currentLogic}
					consumed = 4
				}
			} else if op == "IN" || op == "NOT" {
				finalOp := "IN"
				p := i + 2
				if op == "NOT" {
					finalOp = "NOT IN"
					p = i + 3 // key NOT IN ...
				}

				// ( v1, v2 )
				if tokens[p] == "(" {
					p++
					vals := []interface{}{}
					for tokens[p] != ")" {
						if tokens[p] != "," {
							vals = append(vals, normalizeVal(tokens[p]))
						}
						p++
					}
					consumed = (p - i) + 1
					cond = &Criteria{Key: key, Op: finalOp, Val: vals, Logic: currentLogic}
				}
			} else {
				// Simple op
				val := normalizeVal(tokens[i+2])
				cond = &Criteria{Key: key, Op: op, Val: val, Logic: currentLogic}
				consumed = 3
			}

			conditions = append(conditions, cond)
			i += consumed
		} else {
			break
		}
	}

	*refIndex = i

	if len(conditions) == 1 {
		return conditions[0]
	}
	return &Criteria{Type: "compound", Conditions: conditions}
}

func normalizeVal(valStr string) interface{} {
	if strings.HasPrefix(valStr, "'") || strings.HasPrefix(valStr, "\"") {
		return valStr[1 : len(valStr)-1]
	} else {
		if f, err := strconv.ParseFloat(valStr, 64); err == nil {
			return f
		}
	}
	return valStr
}

func (qp *QueryParser) parseDelete(tokens []string) (*Command, error) {
	var table string
	i := 0
	if strings.ToUpper(tokens[0]) == "DELETE" {
		if strings.ToUpper(tokens[1]) != "FROM" {
			return nil, errors.New("Syntax: DELETE FROM [table]")
		}
		table = tokens[2]
		i = 3
	} else {
		if strings.ToUpper(tokens[1]) != "DARI" {
			return nil, errors.New("Syntax: GUSUR DARI [kebun]")
		}
		table = tokens[2]
		i = 3
	}

	var criteria *Criteria
	if i < len(tokens) {
		upper := strings.ToUpper(tokens[i])
		if upper == "DIMANA" || upper == "WHERE" {
			i++
			criteria = qp.parseWhere(tokens, &i)
		}
	}
	return &Command{Type: "DELETE", Table: table, Criteria: criteria}, nil
}

func (qp *QueryParser) parseUpdate(tokens []string) (*Command, error) {
	var table string
	i := 0
	if strings.ToUpper(tokens[0]) == "UPDATE" {
		table = tokens[1]
		if strings.ToUpper(tokens[2]) != "SET" {
			return nil, errors.New("Expected SET")
		}
		i = 3
	} else {
		table = tokens[1]
		if strings.ToUpper(tokens[2]) != "DENGAN" {
			return nil, errors.New("Expected DENGAN")
		}
		i = 3
	}

	updates := make(map[string]interface{})
	for i < len(tokens) {
		upper := strings.ToUpper(tokens[i])
		if upper == "DIMANA" || upper == "WHERE" {
			break
		}
		if tokens[i] == "," {
			i++
			continue
		}

		key := tokens[i]
		// skip =
		val := normalizeVal(tokens[i+2])
		updates[key] = val
		i += 3
	}

	var criteria *Criteria
	if i < len(tokens) {
		i++ // WHERE
		criteria = qp.parseWhere(tokens, &i)
	}

	return &Command{Type: "UPDATE", Table: table, Updates: updates, Criteria: criteria}, nil
}

func (qp *QueryParser) parseCreateIndex(tokens []string) (*Command, error) {
	// CREATE INDEX ... ON tbl ( field )
	if strings.ToUpper(tokens[0]) == "CREATE" {
		i := 2
		if strings.ToUpper(tokens[i]) != "ON" && len(tokens) > i+1 && strings.ToUpper(tokens[i+1]) == "ON" {
			i++ // skip name
		}
		i++ // ON
		table := tokens[i]
		i++
		i++ // (
		field := tokens[i]
		// ) done
		return &Command{Type: "CREATE_INDEX", Table: table, Field: field}, nil
	}
	// INDEKS table PADA field
	return &Command{Type: "CREATE_INDEX", Table: tokens[1], Field: tokens[3]}, nil
}

func (qp *QueryParser) parseAggregate(tokens []string) (*Command, error) {
	i := 1
	funcValid := strings.ToUpper(tokens[i])
	i++
	i++ // (
	aggField := tokens[i]
	if aggField == "*" {
		aggField = ""
	} // Logic in JS handles null
	i++
	i++ // )
	i++ // DARI
	table := tokens[i]
	i++

	var criteria *Criteria
	if i < len(tokens) && (strings.ToUpper(tokens[i]) == "DIMANA" || strings.ToUpper(tokens[i]) == "WHERE") {
		i++
		criteria = qp.parseWhere(tokens, &i)
	}

	groupBy := ""
	if i < len(tokens) && (strings.ToUpper(tokens[i]) == "KELOMPOK" || strings.ToUpper(tokens[i]) == "GROUP") {
		// GROUP BY
		if strings.ToUpper(tokens[i]) == "GROUP" {
			i++
		}
		i++
		groupBy = tokens[i]
	}

	return &Command{Type: "AGGREGATE", Table: table, Func: funcValid, Field: aggField, Criteria: criteria, GroupBy: groupBy}, nil
}

func (qp *QueryParser) bindParameters(command *Command, params map[string]interface{}) {
	bindValue := func(val interface{}) interface{} {
		if s, ok := val.(string); ok && strings.HasPrefix(s, "@") {
			name := s[1:]
			if v, ok := params[name]; ok {
				return v
			}
		}
		return val
	}

	if command.Criteria != nil {
		qp.bindCriteria(command.Criteria, bindValue)
	}
	if command.Data != nil {
		for k, v := range command.Data {
			command.Data[k] = bindValue(v)
		}
	}
}

func (qp *QueryParser) bindCriteria(c *Criteria, bindFunc func(interface{}) interface{}) {
	if c.Type == "compound" {
		for _, sub := range c.Conditions {
			qp.bindCriteria(sub, bindFunc)
		}
	} else {
		if arr, ok := c.Val.([]interface{}); ok {
			for i, v := range arr {
				arr[i] = bindFunc(v)
			}
		} else {
			c.Val = bindFunc(c.Val)
		}
	}
}
