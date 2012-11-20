package handlersocket

import (
	"bufio"
	"net"
	"strconv"
)

type HandlerSocket interface {
	Auth(secret string) error
	Index(id int, db, table, idx string, columns string) error
	Find(id int, op string, args ...interface{}) error
	FindMulti(id, limit, offset int, op string, args ...interface{}) error
	FindIn(id, limit, offset int, args []interface{}, kcol int, kv []interface{}) error
	Insert(id int, args ...interface{}) error
	Flush() error
	Response() ([]string, error)
	Close() error
}

type handlerSocket struct {
	addr string
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func Dial(addr string) (HandlerSocket, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &handlerSocket{
		addr: addr,
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}, nil
}

// send auth request
func (hs *handlerSocket) Auth(secret string) error {
	return mput(hs.w, 0x0A, "A", "1", secret)
}

// send open_index request
func (hs *handlerSocket) Index(id int, db, table, idx string, columns string) error {
	return mput(hs.w, 0x0A, "P", strconv.Itoa(id), db, table, idx, columns)
}

// send find request
func (hs *handlerSocket) Find(id int, op string, args ...interface{}) (err error) {
	err = mput(hs.w, 0x09, id, op, len(args))
	if err != nil {
		return
	}
	err = mput(hs.w, 0x0A, args...)
	return
}

// send find request with limit and offset parameters
func (hs *handlerSocket) FindMulti(id, limit, offset int, op string, args ...interface{}) (err error) {
	err = mput(hs.w, 0x09, id, op, len(args))
	if err != nil {
		return
	}
	err = mput(hs.w, 0x09, args...)
	if err != nil {
		return
	}
	err = mput(hs.w, 0x0A, limit, offset)
	return
}

// send find in request
func (hs *handlerSocket) FindIn(id, limit, offset int, args []interface{}, kcol int, kv []interface{}) (err error) {
	// Find In seems only to work with = operator
	err = mput(hs.w, 0x09, id, "=", len(args))
	if err != nil {
		return
	}
	err = mput(hs.w, 0x09, args...)
	if err != nil {
		return
	}
	err = mput(hs.w, 0x09, limit, offset, "@", kcol, len(kv))
	if err != nil {
		return
	}
	err = mput(hs.w, 0x0A, kv...)
	return
}

// send insert request
func (hs *handlerSocket) Insert(id int, args ...interface{}) (err error) {
	err = mput(hs.w, 0x09, id, "+", len(args))
	if err != nil {
		return
	}
	err = mput(hs.w, 0x0A, args...)
	return
}

// flush previous writes 
func (hs *handlerSocket) Flush() error {
	return hs.w.Flush()
}

// read one response line
func (hs *handlerSocket) Response() (rsp []string, err error) {
	line, err := parse(hs.r)
	if err != nil {
		return
	}
	rsp, err = check(line)
	return
}

func (hs *handlerSocket) Close() error {
	return hs.conn.Close()
}
