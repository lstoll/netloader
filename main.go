package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func main() {
	concurrentResolversStr := os.Getenv("CONCURRENT_RESOLVERS")
	concurrentGettersStr := os.Getenv("CONCURRENT_GETTERS")

	concurrentResolvers, err := strconv.Atoi(concurrentResolversStr)
	if err != nil {
		fmt.Println("No CONCURRENT_RESOLVERS set, defaulting to 0")
	}
	concurrentGetters, err := strconv.Atoi(concurrentGettersStr)
	if err != nil {
		fmt.Println("No CONCURRENT_GETTERS set, defaulting to 0")
	}

	domains := make(chan string, 1000)
	// Filter this out in to another chan. We never want to slow down
	// the DNS lookups for http requests, so we can just drop if this
	// is full
	getHosts := make(chan string, 1000)

	// The reader
	go func() {
	}()

	// The resolvers
	for i := 0; i < concurrentResolvers; i++ {
		go func() {
			for domain := range domains {
				_, err := net.ResolveIPAddr("ip4", domain)
				if err != nil && strings.HasSuffix(err.Error(), "no such host") {
					// it's fine. it happens
				} else if err != nil {
					fmt.Printf("error looking up domain %s: %s\n", domain, err)
				} else {
					select {
					case getHosts <- domain:
					}
				}
			}
		}()
	}

	// The resolvers
	for i := 0; i < concurrentGetters; i++ {
		go func() {
			for domain := range getHosts {
				resp, err := http.Get("http://" + domain)
				if err != nil {
					fmt.Printf("error getting domain %s over http: %s\n", domain, err)
				} else {
					if resp != nil && resp.Body != nil {
						_, err := io.Copy(ioutil.Discard, resp.Body)
						if err != nil {
							fmt.Printf("error reading body for %s to discard: %s\n", domain, err)
						}
						resp.Body.Close()
					}
				}
			}
		}()
	}

	// The reader
	for {
		f, err := os.Open("top-1m.csv")
		if err != nil {
			panic(err)
		}
		r := csv.NewReader(bufio.NewReader(f))
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			// rank,domain name
			domains <- record[1]
		}
		f.Close()
	}

}
