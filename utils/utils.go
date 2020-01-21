package utils

import "testing"

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func AssertnumResult(t *testing.T, want, get int64) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func AssertfloatResult(t *testing.T, want, get float64) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func AssertFalse(t *testing.T, v bool) {
	t.Helper()
	if v == true {
		t.Error("assert false but get a true value")
	}
}

func AssertTrue(t *testing.T, v bool) {
	t.Helper()
	if v != true {
		t.Error("assert false but get a true value")
	}
}
