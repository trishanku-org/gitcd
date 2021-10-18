#!/bin/bash

cat > pkg/mocks/git/generate.go <<EOF
// Code generated by hack/meta-generate.sh. DO NOT EDIT.
//go:generate mockgen -package git -destination mocks.go github.com/trishanku/gitcd/pkg/git $(grep 'interface {' ./pkg/git/types.go | sort | awk '{ print $2 }' | tr '\n' ',' | sed 's/,$//')
package git
EOF