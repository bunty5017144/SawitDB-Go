package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type SawitClient struct {
	ConnectionString string
	Host             string
	Port             int
	Database         string
	Username         string
	Password         string
	Conn             net.Conn
	Reader           *bufio.Reader
}

func NewSawitClient(connStr string) *SawitClient {
	c := &SawitClient{ConnectionString: connStr}
	c.parseConnectionString(connStr)
	return c
}

func (c *SawitClient) parseConnectionString(connStr string) {
	// sawitdb://[user:pass@]host:port/database
	uStr := strings.Replace(connStr, "sawitdb://", "http://", 1)
	u, err := url.Parse(uStr)
	if err != nil {
		c.Host = "localhost"
		c.Port = 7878
		return
	}

	c.Host = u.Hostname()
	c.Port = 7878
	if p := u.Port(); p != "" {
		c.Port, _ = strconv.Atoi(p)
	}
	c.Database = strings.TrimPrefix(u.Path, "/")
	if u.User != nil {
		c.Username = u.User.Username()
		c.Password, _ = u.User.Password()
	}
}

func (c *SawitClient) Connect() error {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.Conn = conn
	c.Reader = bufio.NewReader(conn)

	// Read Welcome
	_, err = c.readResponse() // consume welcome
	if err != nil {
		return err
	}

	return c.initConnection()
}

func (c *SawitClient) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

func (c *SawitClient) initConnection() error {
	if c.Username != "" && c.Password != "" {
		if err := c.authenticate(); err != nil {
			return err
		}
	}
	if c.Database != "" {
		if _, err := c.Use(c.Database); err != nil {
			return err
		}
	}
	return nil
}

func (c *SawitClient) sendRequest(reqType string, payload map[string]interface{}) (map[string]interface{}, error) {
	req := map[string]interface{}{
		"type":    reqType,
		"payload": payload,
	}
	bytes, _ := json.Marshal(req)
	if _, err := c.Conn.Write(append(bytes, '\n')); err != nil {
		return nil, err
	}
	return c.readResponse()
}

func (c *SawitClient) readResponse() (map[string]interface{}, error) {
	line, err := c.Reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	var res map[string]interface{}
	if err := json.Unmarshal([]byte(line), &res); err != nil {
		return nil, err
	}

	if resType, ok := res["type"].(string); ok && resType == "error" {
		errMsg, _ := res["error"].(string)
		return nil, errors.New(errMsg)
	}
	return res, nil
}

func (c *SawitClient) authenticate() error {
	_, err := c.sendRequest("auth", map[string]interface{}{
		"username": c.Username,
		"password": c.Password,
	})
	return err
}

func (c *SawitClient) Use(database string) (string, error) {
	res, err := c.sendRequest("use", map[string]interface{}{"database": database})
	if err != nil {
		return "", err
	}
	msg, _ := res["message"].(string)
	return msg, nil
}

func (c *SawitClient) Query(query string, params map[string]interface{}) (interface{}, error) {
	res, err := c.sendRequest("query", map[string]interface{}{
		"query":  query,
		"params": params,
	})
	if err != nil {
		return nil, err
	}
	return res["result"], nil
}

func (c *SawitClient) ListDatabases() ([]string, error) {
	res, err := c.sendRequest("list_databases", nil)
	if err != nil {
		return nil, err
	}

	rawList, _ := res["databases"].([]interface{})
	list := make([]string, len(rawList))
	for i, v := range rawList {
		list[i], _ = v.(string)
	}
	return list, nil
}

func (c *SawitClient) Ping() (int64, error) {
	start := time.Now()
	_, err := c.sendRequest("ping", nil)
	return time.Since(start).Milliseconds(), err
}
