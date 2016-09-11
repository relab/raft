package main

import "fmt"

type nodeFlags []string

func (nf *nodeFlags) String() string {
	return "raft node"
}

func (nf *nodeFlags) Set(value string) error {
	*nf = append(*nf, fmt.Sprintf(":%s", value))

	return nil
}
