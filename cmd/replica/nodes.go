package main

import (
	"fmt"
	"strings"
)

type Nodes []string

func (n *Nodes) String() string {
	return strings.Join(*n, ",")
}

func (n *Nodes) Set(value string) error {
	*n = append(*n, fmt.Sprintf(":%s", value))

	return nil
}
