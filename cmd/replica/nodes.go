package main

import "strings"

type Nodes []string

func (n *Nodes) String() string {
	return strings.Join(*n, ",")
}

func (n *Nodes) Set(value string) error {
	*n = append(*n, value)

	return nil
}
