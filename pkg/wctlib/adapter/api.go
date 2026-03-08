package adapter

import (
	"fmt"
	"strings"
)

const APIVersion = "wctlib-adapter/v1"

func (r *Registry) Size() int {
	return len(r.list())
}

func (r *Registry) DebugString() string {
	items := r.list()
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, fmt.Sprintf("%s@%s", item.ID(), item.Version()))
	}
	return "adapters: " + strings.Join(ids, ",")
}
