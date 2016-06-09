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
	"strings"
)

var ConcurrentResolvers = 32
var ConcurrentGetters = 16

func main() {
	domains := make(chan string, 1000)
	// Filter this out in to another chan. We never want to slow down
	// the DNS lookups for http requests, so we can just drop if this
	// is full
	getHosts := make(chan string, 1000)

	// The reader
	go func() {
	}()

	// The resolvers
	for i := 0; i < ConcurrentResolvers; i++ {
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
	for i := 0; i < ConcurrentGetters; i++ {
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
