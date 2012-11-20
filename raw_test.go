package handlersocket

import (
	"testing"
)

/*
Required test database tables:

CREATE TABLE test.kv (
	id int auto_increment,
	col varchar(20) not null,
	primary key(id)
) Engine=InnoDB;
*/

func TestRequest(t *testing.T) {
	var err error
	hs, err := Dial("pangolin:9999")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Auth("HandlerSocketSecret")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Flush()
	if err != nil {
		t.Fatal(err)
	}
	_, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}

	err = hs.Index(1, "test", "kv", "PRIMARY", "col")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Flush()
	if err != nil {
		t.Fatal(err)
	}
	_, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}

	err = hs.Index(2, "test", "kv", "PRIMARY", "id,col")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Flush()
	if err != nil {
		t.Fatal(err)
	}
	_, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}

	err = hs.Insert(1, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Flush()
	if err != nil {
		t.Fatal(err)
	}
	_, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}

	err = hs.Find(2, "=", "1")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Find(2, "=", "2")
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Flush()
	if err != nil {
		t.Fatal(err)
	}
	rsp, err := hs.Response()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", rsp)
	rsp, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", rsp)

	err = hs.FindMulti(2, 5, 0, ">", 2)
	if err != nil {
		t.Fatal(err)
	}
	hs.Flush()
	rsp, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", rsp)

	err = hs.FindIn(2, 10, 0, []interface{}{""}, 0, []interface{}{3, 6, 9})
	if err != nil {
		t.Fatal(err)
	}
	err = hs.Flush()
	if err != nil {
		t.Fatal(err)
	}
	rsp, err = hs.Response()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", rsp)

	err = hs.Close()
	if err != nil {
		t.Fatal(err)
	}
}
