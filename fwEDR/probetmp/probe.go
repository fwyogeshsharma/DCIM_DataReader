package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gosnmp/gosnmp"
)

func main() {
	comm := "192.168.1.6"
	if len(os.Args) > 1 {
		comm = os.Args[1]
	}
	port := uint16(161)
	if len(os.Args) > 2 {
		var p int
		fmt.Sscanf(os.Args[2], "%d", &p)
		port = uint16(p)
	}
	g := &gosnmp.GoSNMP{
		Target:    "127.0.0.1",
		Port:      port,
		Community: comm,
		Version:   gosnmp.Version2c,
		Timeout:   3 * time.Second,
		Retries:   0,
	}
	if err := g.Connect(); err != nil {
		fmt.Println("CONNECT ERR:", err)
		return
	}
	defer g.Conn.Close()
	start := time.Now()
	r, err := g.Get([]string{"1.3.6.1.2.1.1.5.0", "1.3.6.1.2.1.1.3.0"})
	if err != nil {
		fmt.Printf("GET ERR after %v: %v\n", time.Since(start), err)
		return
	}
	fmt.Printf("RESPONSE in %v (community=%s):\n", time.Since(start), comm)
	for _, v := range r.Variables {
		fmt.Printf("  %s = %v\n", v.Name, v.Value)
	}
}
