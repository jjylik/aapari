package db_test

import (
	"bytes"
	"jjylik/aapari/db"
	"os"
	"testing"
)

func createDB() (*db.DB, error) {
	os.Remove("./unit.db")
	db, err := db.Open(2, 16, 0.8, "./unit.db")
	return db, err
}

func TestDBPutCreate(t *testing.T) {
	db, err := createDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestDBPutUpdate(t *testing.T) {
	db, err := createDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put([]byte("key"), []byte("expected"))
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := db.Get([]byte("key"))
	t.Log(value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("expected")) {
		t.Fatal("value is not equal to expected")
	}
}

func TestDBGet(t *testing.T) {
	db, err := createDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Put([]byte("key"), []byte("expected"))
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("expected")) {
		t.Fatal("value is not equal to expected")
	}
}

func TestDBDelete(t *testing.T) {
	db, err := createDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Put([]byte("key"), []byte("expected"))
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("expected")) {
		t.Fatal("value is not equal to expected")
	}
	ok, err := db.Delete([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("delete failed")
	}
	value, ok, err = db.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatal("value is not nil")
	}
	if ok {
		t.Fatal("get returned a key that was deleted")
	}
}
