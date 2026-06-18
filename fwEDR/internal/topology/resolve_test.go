package topology

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, path, body string, mod time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
}

func TestResolveSingleFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "only.json")
	write(t, p, `{"nodes":[{"id":"a"}],"edges":[]}`, time.Now())

	ti, err := Resolve(p) // explicit file
	if err != nil {
		t.Fatal(err)
	}
	if ti.Name != "only" || ti.Nodes != 1 || ti.Edges != 0 {
		t.Fatalf("unexpected: %+v", ti)
	}
	if len(ti.Hash) != 12 {
		t.Fatalf("hash len = %d, want 12 (%q)", len(ti.Hash), ti.Hash)
	}
}

func TestResolveDirNewestWins(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "old.json")
	newer := filepath.Join(dir, "new.json")
	write(t, old, `{"nodes":[{"id":"a"}],"edges":[]}`, time.Now().Add(-time.Hour))
	write(t, newer, `{"nodes":[{"id":"a"},{"id":"b"}],"edges":[{"src":"a","dst":"b"}]}`, time.Now())

	ti, err := Resolve(dir) // directory → newest-modified wins
	if err != nil {
		t.Fatal(err)
	}
	if ti.Name != "new" {
		t.Fatalf("chose %q, want newest 'new'", ti.Name)
	}
	if ti.Nodes != 2 || ti.Edges != 1 {
		t.Fatalf("counts off: %+v", ti)
	}
}

func TestResolveDirEmpty(t *testing.T) {
	if _, err := Resolve(t.TempDir()); err == nil {
		t.Fatal("expected error for directory with no *.json")
	}
}

func TestResolveHashChangesWithContent(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "t.json")
	write(t, p, `{"nodes":[{"id":"a"}],"edges":[]}`, time.Now())
	a, _ := Resolve(p)
	write(t, p, `{"nodes":[{"id":"a"},{"id":"b"}],"edges":[]}`, time.Now())
	b, _ := Resolve(p)
	if a.Hash == b.Hash {
		t.Fatal("hash should change when content changes")
	}
}
