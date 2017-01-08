package main

import "strings"

// Nodes is a slice of server addresses.
type Nodes []string

func (n *Nodes) String() string {
	return strings.Join(*n, ",")
}

// Set appends multiple node arguments to one slice.
func (n *Nodes) Set(value string) error {
	*n = append(*n, value)

	return nil
}
