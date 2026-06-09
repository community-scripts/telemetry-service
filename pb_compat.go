package main

import (
	"context"
	"net/http"
)

// PBClient is a legacy compatibility shim kept so old PocketBase-oriented
// helper methods in dashboard.go still compile while the service uses CHClient.
// New code should use CHClient directly.
type PBClient struct {
	baseURL    string
	targetColl string
	token      string
	http       *http.Client
}

// ensureAuth is retained for legacy call sites; ClickHouse-backed runtime paths
// do not use PBClient.
func (p *PBClient) ensureAuth(ctx context.Context) error {
	return nil
}
