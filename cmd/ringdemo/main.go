// ringdemo displays deterministic key-to-primary placement for sample keys.
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/pranavb1234/dkvStore/internal/ring"
)

func main() {
	nodeList := flag.String("nodes", "node1,node2,node3", "comma-separated node IDs")
	virtualNodes := flag.Int("virtual-nodes", 64, "virtual nodes per physical node")
	flag.Parse()

	hashRing, err := ring.New(*virtualNodes)
	if err != nil {
		log.Fatal(err)
	}
	for _, nodeID := range strings.Split(*nodeList, ",") {
		if err := hashRing.Add(nodeID); err != nil {
			log.Fatal(err)
		}
	}

	keys := flag.Args()
	if len(keys) == 0 {
		keys = []string{"alpha", "beta", "course", "student-42"}
	}
	for _, key := range keys {
		primary, _ := hashRing.Primary(key)
		fmt.Printf("%s -> %s\n", key, primary)
	}
}
