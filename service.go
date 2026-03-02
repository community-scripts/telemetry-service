package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

//go:embed public
var publicFS embed.FS

type Config struct {
	ListenAddr         string
	TrustedProxiesCIDR []string

	// PocketBase
	PBBaseURL        string
	PBAuthCollection string // PB auth collection name (from env)
	PBIdentity       string // email
	PBPassword       string
	PBTargetColl     string // PB data collection name (from env)

	// Limits
	MaxBodyBytes     int64
	RateLimitRPM     int           // requests per minute per key
	RateBurst        int           // burst tokens
	RateKeyMode      string        // "ip" or "header"
	RateKeyHeader    string        // e.g. "X-Telemetry-Key"
	RequestTimeout   time.Duration // upstream timeout
	EnableReqLogging bool          // default false (GDPR-friendly)

	// Cache
	RedisURL     string
	EnableRedis  bool
	CacheTTL     time.Duration
	CacheEnabled bool

	// Alerts (SMTP)
	AlertEnabled          bool
	SMTPHost              string
	SMTPPort              int
	SMTPUser              string
	SMTPPassword          string
	SMTPFrom              string
	SMTPTo                []string
	SMTPUseTLS            bool
	AlertFailureThreshold float64
	AlertCheckInterval    time.Duration
	AlertCooldown         time.Duration

	// GitHub Integration
	GitHubToken   string // Personal access token for creating issues
	GitHubOwner   string // Repository owner (e.g., "community-scripts")
	GitHubRepo    string // Repository name (e.g., "ProxmoxVE")
	AdminPassword string // Password to protect admin actions (issue creation)
}

// TelemetryIn matches payload from api.func (bash client)
type TelemetryIn struct {
	// Required
	RandomID    string `json:"random_id"`              // Session UUID
	ExecutionID string `json:"execution_id,omitempty"` // Unique execution ID (unique-indexed in PocketBase)
	Type        string `json:"type"`                   // "lxc", "vm", "pve", "addon"
	NSAPP       string `json:"nsapp"`                  // Application name (e.g., "jellyfin")
	Status      string `json:"status"`                 // "installing", "success", "failed", "aborted", "unknown"

	// Container/VM specs
	CTType    int `json:"ct_type,omitempty"`    // 1=unprivileged, 2=privileged/VM
	DiskSize  int `json:"disk_size,omitempty"`  // GB
	CoreCount int `json:"core_count,omitempty"` // CPU cores
	RAMSize   int `json:"ram_size,omitempty"`   // MB

	// System info
	OsType    string `json:"os_type,omitempty"`    // "debian", "ubuntu", "alpine", etc.
	OsVersion string `json:"os_version,omitempty"` // "12", "24.04", etc.
	PveVer    string `json:"pve_version,omitempty"`

	// Optional
	Method   string `json:"method,omitempty"`    // "default", "advanced"
	Error    string `json:"error,omitempty"`     // Error description (max 120 chars)
	ExitCode int    `json:"exit_code,omitempty"` // 0-255

	// === EXTENDED FIELDS ===

	// GPU Passthrough stats
	GPUVendor      string `json:"gpu_vendor,omitempty"`      // "intel", "amd", "nvidia"
	GPUModel       string `json:"gpu_model,omitempty"`       // e.g., "Intel Arc Graphics"
	GPUPassthrough string `json:"gpu_passthrough,omitempty"` // "igpu", "dgpu", "vgpu", "none"

	// CPU stats
	CPUVendor string `json:"cpu_vendor,omitempty"` // "intel", "amd", "arm"
	CPUModel  string `json:"cpu_model,omitempty"`  // e.g., "Intel Core Ultra 7 155H"

	// RAM stats
	RAMSpeed string `json:"ram_speed,omitempty"` // e.g., "4800" (MT/s)

	// Performance metrics
	InstallDuration int `json:"install_duration,omitempty"` // Seconds

	// Error categorization
	ErrorCategory string `json:"error_category,omitempty"` // "network", "storage", "dependency", "permission", "timeout", "unknown"

	// Repository source for collection routing
	RepoSource string `json:"repo_source,omitempty"` // "ProxmoxVE", "ProxmoxVED", or "external"
}

// TelemetryOut is sent to PocketBase (matches telemetry collection)
type TelemetryOut struct {
	RandomID    string `json:"random_id"`
	ExecutionID string `json:"execution_id,omitempty"`
	Type        string `json:"type"`
	NSAPP       string `json:"nsapp"`
	Status      string `json:"status"`
	CTType      int    `json:"ct_type,omitempty"`
	DiskSize    int    `json:"disk_size,omitempty"`
	CoreCount   int    `json:"core_count,omitempty"`
	RAMSize     int    `json:"ram_size,omitempty"`
	OsType      string `json:"os_type,omitempty"`
	OsVersion   string `json:"os_version,omitempty"`
	PveVer      string `json:"pve_version,omitempty"`
	Method      string `json:"method,omitempty"`
	Error       string `json:"error,omitempty"`
	ExitCode    int    `json:"exit_code,omitempty"`

	// Extended fields
	GPUVendor       string `json:"gpu_vendor,omitempty"`
	GPUModel        string `json:"gpu_model,omitempty"`
	GPUPassthrough  string `json:"gpu_passthrough,omitempty"`
	CPUVendor       string `json:"cpu_vendor,omitempty"`
	CPUModel        string `json:"cpu_model,omitempty"`
	RAMSpeed        string `json:"ram_speed,omitempty"`
	InstallDuration int    `json:"install_duration,omitempty"`
	ErrorCategory   string `json:"error_category,omitempty"`

	// Repository source: "ProxmoxVE", "ProxmoxVED", or "external"
	RepoSource string `json:"repo_source,omitempty"`

	// Pipeline: JSON array of status transitions [{"s":"installing","t":"..."},…]
	Pipeline json.RawMessage `json:"pipeline,omitempty"`
}

// TelemetryStatusUpdate contains only fields needed for status updates
type TelemetryStatusUpdate struct {
	Status          string          `json:"status"`
	ExecutionID     string          `json:"execution_id,omitempty"`
	Error           string          `json:"error,omitempty"`
	ExitCode        int             `json:"exit_code"`
	InstallDuration int             `json:"install_duration,omitempty"`
	ErrorCategory   string          `json:"error_category,omitempty"`
	GPUVendor       string          `json:"gpu_vendor,omitempty"`
	GPUModel        string          `json:"gpu_model,omitempty"`
	GPUPassthrough  string          `json:"gpu_passthrough,omitempty"`
	CPUVendor       string          `json:"cpu_vendor,omitempty"`
	CPUModel        string          `json:"cpu_model,omitempty"`
	RAMSpeed        string          `json:"ram_speed,omitempty"`
	RepoSource      string          `json:"repo_source,omitempty"`
	Pipeline        json.RawMessage `json:"pipeline,omitempty"`
}

// PipelineStep represents a single step in the installation pipeline history.
type PipelineStep struct {
	Status string `json:"s"`
	Time   string `json:"t"`
}

// pipelineTracker keeps in-memory pipeline state for active installations.
// Key: execution_id (or random_id), Value: JSON string of []PipelineStep.
// Entries are cleaned up when a terminal status (success/failed/aborted) is received.
var pipelineTracker sync.Map

// pipelineTrackingKey returns the key used to track pipeline state for an installation.
func pipelineTrackingKey(executionID, randomID string) string {
	if executionID != "" {
		return executionID
	}
	return randomID
}

// trackPipelineStep appends a status step to the in-memory pipeline and returns the updated JSON.
func trackPipelineStep(trackingKey, status string) json.RawMessage {
	now := time.Now().UTC().Format(time.RFC3339)
	step := PipelineStep{Status: status, Time: now}

	var steps []PipelineStep
	if existing, ok := pipelineTracker.Load(trackingKey); ok {
		_ = json.Unmarshal(existing.(json.RawMessage), &steps)
	}
	steps = append(steps, step)

	data, _ := json.Marshal(steps)
	pipelineTracker.Store(trackingKey, json.RawMessage(data))
	return json.RawMessage(data)
}

// cleanupPipeline removes the in-memory pipeline state for a completed installation.
func cleanupPipeline(trackingKey string) {
	pipelineTracker.Delete(trackingKey)
}

// Allowed values for 'repo_source' field
var allowedRepoSource = map[string]bool{
	"ProxmoxVE":  true,
	"ProxmoxVED": true,
	"external":   true,
}

type PBClient struct {
	baseURL        string
	authCollection string
	identity       string
	password       string
	targetColl     string // single collection for all telemetry data

	mu    sync.Mutex
	token string
	exp   time.Time
	http  *http.Client
}

func NewPBClient(cfg Config) *PBClient {
	return &PBClient{
		baseURL:        strings.TrimRight(cfg.PBBaseURL, "/"),
		authCollection: cfg.PBAuthCollection,
		identity:       cfg.PBIdentity,
		password:       cfg.PBPassword,
		targetColl:     cfg.PBTargetColl,
		http: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
	}
}

func (p *PBClient) ensureAuth(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// refresh if token missing or expiring soon
	if p.token != "" && time.Until(p.exp) > 60*time.Second {
		return nil
	}

	body := map[string]string{
		"identity": p.identity,
		"password": p.password,
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/collections/%s/auth-with-password", p.baseURL, p.authCollection),
		bytes.NewReader(b),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("pocketbase auth failed: %s: %s", resp.Status, strings.TrimSpace(string(rb)))
	}

	var out struct {
		Token string `json:"token"`
		// record omitted
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if out.Token == "" {
		return errors.New("pocketbase auth token missing")
	}

	// PocketBase JWT exp can be parsed, but keep it simple: set 50 min
	p.token = out.Token
	p.exp = time.Now().Add(50 * time.Minute)
	return nil
}

// FindRecordByRandomID searches for an existing record by random_id
func (p *PBClient) FindRecordByRandomID(ctx context.Context, randomID string) (string, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return "", err
	}

	// URL encode the filter to ensure special characters are handled correctly
	filter := fmt.Sprintf("random_id='%s'", randomID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/collections/%s/records?filter=%s&fields=id&perPage=1",
			p.baseURL, p.targetColl, url.QueryEscape(filter)),
		nil,
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("pocketbase search failed: %s", resp.Status)
	}

	var result struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if len(result.Items) == 0 {
		return "", nil // Not found
	}
	return result.Items[0].ID, nil
}

// FindRecordByExecutionID searches for an existing record by execution_id (unique-indexed, O(1) lookup)
func (p *PBClient) FindRecordByExecutionID(ctx context.Context, executionID string) (string, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return "", err
	}

	filter := fmt.Sprintf("execution_id='%s'", executionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/collections/%s/records?filter=%s&fields=id&perPage=1",
			p.baseURL, p.targetColl, url.QueryEscape(filter)),
		nil,
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("pocketbase search by execution_id failed: %s", resp.Status)
	}

	var result struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if len(result.Items) == 0 {
		return "", nil // Not found
	}
	return result.Items[0].ID, nil
}

// UpdateTelemetryStatus updates only status, error, and exit_code of an existing record
func (p *PBClient) UpdateTelemetryStatus(ctx context.Context, recordID string, update TelemetryStatusUpdate) error {
	if err := p.ensureAuth(ctx); err != nil {
		return err
	}

	b, _ := json.Marshal(update)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch,
		fmt.Sprintf("%s/api/collections/%s/records/%s", p.baseURL, p.targetColl, recordID),
		bytes.NewReader(b),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("pocketbase update failed: %s: %s", resp.Status, strings.TrimSpace(string(rb)))
	}
	return nil
}

// UpdateTelemetryFull updates an existing record with ALL fields from the payload.
// Used for terminal states (success/failed/aborted) to fill in any missing spec fields
// from records that were created as fallbacks from configuring/validation pings.
// Fields with omitempty+zero values are omitted from JSON, preserving existing PB values.
func (p *PBClient) UpdateTelemetryFull(ctx context.Context, recordID string, payload TelemetryOut) error {
	if err := p.ensureAuth(ctx); err != nil {
		return err
	}

	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch,
		fmt.Sprintf("%s/api/collections/%s/records/%s", p.baseURL, p.targetColl, recordID),
		bytes.NewReader(b),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("pocketbase full-update failed: %s: %s", resp.Status, strings.TrimSpace(string(rb)))
	}
	return nil
}

// EnsurePipelineField checks the PocketBase collection schema and adds the
// 'pipeline' JSON field if it does not yet exist. Called once on startup.
// Supports both PB <0.22 ("schema" key) and PB >=0.22 ("fields" key).
func (p *PBClient) EnsurePipelineField(ctx context.Context) error {
	if err := p.ensureAuth(ctx); err != nil {
		return err
	}

	// Read current collection definition
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/collections/%s", p.baseURL, p.targetColl), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("read collection schema: HTTP %s", resp.Status)
	}

	// PB <0.22 uses "schema", PB >=0.22 uses "fields" for field definitions.
	// Parse both and use whichever is present.
	var raw map[string]json.RawMessage
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("decode collection: %w", err)
	}

	// Determine which key holds the field definitions
	fieldsKey := "fields" // PB >=0.22 default
	fieldsDef, hasFields := raw["fields"]
	schemaDef, hasSchema := raw["schema"]
	if !hasFields && hasSchema {
		fieldsKey = "schema" // PB <0.22 fallback
		fieldsDef = schemaDef
	} else if !hasFields && !hasSchema {
		return fmt.Errorf("collection response has neither 'fields' nor 'schema' key")
	}

	var fields []map[string]interface{}
	if fieldsDef != nil {
		if err := json.Unmarshal(fieldsDef, &fields); err != nil {
			return fmt.Errorf("decode %s array: %w", fieldsKey, err)
		}
	}

	// Check if pipeline field already exists
	for _, field := range fields {
		if name, _ := field["name"].(string); name == "pipeline" {
			log.Printf("[MIGRATION] Pipeline field already exists in '%s' (via %s key)", p.targetColl, fieldsKey)
			return nil
		}
	}

	// Append the pipeline field (PocketBase JSON type)
	log.Printf("[MIGRATION] Adding 'pipeline' JSON field to collection '%s' (using %s key)", p.targetColl, fieldsKey)
	newField := map[string]interface{}{
		"name":     "pipeline",
		"type":     "json",
		"required": false,
	}
	// PB <0.22 expects "options", PB >=0.22 uses "maxSize" directly
	if fieldsKey == "schema" {
		newField["options"] = map[string]interface{}{}
	} else {
		newField["maxSize"] = 0 // 0 = unlimited
	}
	fields = append(fields, newField)

	patchBody, _ := json.Marshal(map[string]interface{}{fieldsKey: fields})
	patchReq, err := http.NewRequestWithContext(ctx, http.MethodPatch,
		fmt.Sprintf("%s/api/collections/%s", p.baseURL, p.targetColl),
		bytes.NewReader(patchBody))
	if err != nil {
		return err
	}
	patchReq.Header.Set("Content-Type", "application/json")
	patchReq.Header.Set("Authorization", "Bearer "+p.token)

	patchResp, err := p.http.Do(patchReq)
	if err != nil {
		return fmt.Errorf("patch collection: %w", err)
	}
	defer patchResp.Body.Close()

	if patchResp.StatusCode < 200 || patchResp.StatusCode >= 300 {
		rb, _ := io.ReadAll(io.LimitReader(patchResp.Body, 4<<10))
		return fmt.Errorf("add pipeline field via %s: %s: %s", fieldsKey, patchResp.Status, string(rb))
	}

	log.Printf("[MIGRATION] Successfully added 'pipeline' field to '%s' via %s key", p.targetColl, fieldsKey)
	return nil
}

// FetchRecordsPaginated retrieves records with pagination and optional filters.
func (p *PBClient) FetchRecordsPaginated(ctx context.Context, page, limit int, status, app, osType, typeFilter, sortField, repoSource string, days int) ([]TelemetryRecord, int, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, 0, err
	}

	// Build filter
	var filters []string
	// Date filter
	if days > 0 {
		var since string
		if days == 1 {
			// "Today" = since midnight today (not yesterday)
			since = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			// N days = today + (N-1) previous days
			since = time.Now().AddDate(0, 0, -(days-1)).Format("2006-01-02") + " 00:00:00"
		}
		filters = append(filters, fmt.Sprintf("created >= '%s'", since))
	}
	if status != "" {
		if status == "aborted" {
			// Include both native "aborted" and legacy "failed" records with SIGINT/SIGHUP indicators
			filters = append(filters, "(status='aborted' || (status='failed' && (exit_code=129 || exit_code=130 || error~'SIGINT' || error~'SIGHUP' || error~'Ctrl+C' || error~'Ctrl-C')))")
		} else if status == "failed" {
			// Exclude SIGINT/SIGHUP records from "failed" (they are reclassified as "aborted")
			// PocketBase negation uses !~ operator, not !(field~value)
			filters = append(filters, "(status='failed' && exit_code!=129 && exit_code!=130 && error!~'SIGINT' && error!~'SIGHUP' && error!~'Ctrl+C' && error!~'Ctrl-C')")
		} else {
			filters = append(filters, fmt.Sprintf("status='%s'", status))
		}
	}
	if app != "" {
		filters = append(filters, fmt.Sprintf("nsapp~'%s'", app))
	}
	if osType != "" {
		filters = append(filters, fmt.Sprintf("os_type='%s'", osType))
	}
	if typeFilter != "" {
		filters = append(filters, fmt.Sprintf("type='%s'", typeFilter))
	}
	if repoSource != "" {
		filters = append(filters, fmt.Sprintf("repo_source='%s'", repoSource))
	}

	filterStr := ""
	if len(filters) > 0 {
		filterStr = "&filter=" + url.QueryEscape(strings.Join(filters, " && "))
	}

	// Handle sort parameter (default: -created)
	sort := "-created"
	if sortField != "" {
		// Validate sort field to prevent injection
		allowedFields := map[string]bool{
			"created": true, "-created": true,
			"nsapp": true, "-nsapp": true,
			"status": true, "-status": true,
			"os_type": true, "-os_type": true,
			"type": true, "-type": true,
			"method": true, "-method": true,
			"exit_code": true, "-exit_code": true,
		}
		if allowedFields[sortField] {
			sort = sortField
		}
	}

	reqURL := fmt.Sprintf("%s/api/collections/%s/records?sort=%s&page=%d&perPage=%d%s",
		p.baseURL, p.targetColl, sort, page, limit, filterStr)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, 0, fmt.Errorf("pocketbase fetch failed: %s", resp.Status)
	}

	var result struct {
		Items      []TelemetryRecord `json:"items"`
		TotalItems int               `json:"totalItems"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, err
	}

	return result.Items, result.TotalItems, nil
}

// UpsertTelemetry handles both creation and updates intelligently.
// All records go to the same collection; repo_source is stored as a field.
//
// For status="installing": always creates a new record.
// For status!="installing": updates existing record.
//   - Prefers execution_id lookup (unique-indexed, O(1)) when available.
//   - Falls back to random_id lookup (filter query) for old clients.
func (p *PBClient) UpsertTelemetry(ctx context.Context, payload TelemetryOut) error {
	// For "installing" status, always create new record
	if payload.Status == "installing" {
		return p.CreateTelemetry(ctx, payload)
	}

	// For status updates (success/failed/unknown), find and update existing record
	// Prefer execution_id (unique-indexed) over random_id (filter query) for faster lookups
	var recordID string
	var err error

	if payload.ExecutionID != "" {
		recordID, err = p.FindRecordByExecutionID(ctx, payload.ExecutionID)
		if err != nil {
			// Execution ID lookup failed, fall back to random_id
			recordID, err = p.FindRecordByRandomID(ctx, payload.RandomID)
		}
	} else {
		// Old client without execution_id — use random_id lookup
		recordID, err = p.FindRecordByRandomID(ctx, payload.RandomID)
	}

	if err != nil {
		// Search failed, log and return error
		return fmt.Errorf("cannot find record to update: %w", err)
	}

	if recordID == "" {
		// Progress pings (validation/configuring) — the "installing" record hasn't
		// arrived yet (race condition, client timeout, or dropped request).
		// Create a minimal record so subsequent updates (success/failed) can find it.
		// The final status update includes full specs and will fill in missing fields.
		if payload.Status == "configuring" || payload.Status == "validation" {
			log.Printf("[WARN] %s update for %s (exec=%s) — no record found, creating fallback",
				payload.Status, payload.NSAPP, payload.ExecutionID)
			return p.CreateTelemetry(ctx, payload)
		}
		// For final states (failed/success/aborted) — create as fallback.
		// post_update_to_api sends full payload so the record will have all fields.
		return p.CreateTelemetry(ctx, payload)
	}

	// Update record. For terminal states (success/failed/aborted), use the full
	// payload so we fill in any missing fields from earlier fallback records.
	// For progress pings, use partial update to avoid overwriting spec fields with zeros.
	isTerminal := payload.Status == "success" || payload.Status == "failed" || payload.Status == "aborted"
	if isTerminal {
		// Full update: `post_update_to_api` sends all fields including specs.
		// Fields with omitempty+zero values are omitted from JSON, preserving existing PB values.
		return p.UpdateTelemetryFull(ctx, recordID, payload)
	}

	// Partial update for progress pings
	update := TelemetryStatusUpdate{
		Status:          payload.Status,
		ExecutionID:     payload.ExecutionID,
		Error:           payload.Error,
		ExitCode:        payload.ExitCode,
		InstallDuration: payload.InstallDuration,
		ErrorCategory:   payload.ErrorCategory,
		GPUVendor:       payload.GPUVendor,
		GPUModel:        payload.GPUModel,
		GPUPassthrough:  payload.GPUPassthrough,
		CPUVendor:       payload.CPUVendor,
		CPUModel:        payload.CPUModel,
		RAMSpeed:        payload.RAMSpeed,
		RepoSource:      payload.RepoSource,
		Pipeline:        payload.Pipeline,
	}
	return p.UpdateTelemetryStatus(ctx, recordID, update)
}

func (p *PBClient) CreateTelemetry(ctx context.Context, payload TelemetryOut) error {
	if err := p.ensureAuth(ctx); err != nil {
		return err
	}

	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/collections/%s/records", p.baseURL, p.targetColl),
		bytes.NewReader(b),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("pocketbase create failed: %s: %s", resp.Status, strings.TrimSpace(string(rb)))
	}
	return nil
}

// -------- Rate limiter (token bucket / minute window, simple) --------

type bucket struct {
	tokens int
	reset  time.Time
}

type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rpm      int
	burst    int
	window   time.Duration
	cleanInt time.Duration
}

func NewRateLimiter(rpm, burst int) *RateLimiter {
	rl := &RateLimiter{
		buckets:  make(map[string]*bucket),
		rpm:      rpm,
		burst:    burst,
		window:   time.Minute,
		cleanInt: 5 * time.Minute,
	}
	go rl.cleanupLoop()
	return rl
}

func (r *RateLimiter) cleanupLoop() {
	t := time.NewTicker(r.cleanInt)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		r.mu.Lock()
		for k, b := range r.buckets {
			if now.After(b.reset.Add(2 * r.window)) {
				delete(r.buckets, k)
			}
		}
		r.mu.Unlock()
	}
}

func (r *RateLimiter) Allow(key string) bool {
	if r.rpm <= 0 {
		return true
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	b, ok := r.buckets[key]
	if !ok || now.After(b.reset) {
		r.buckets[key] = &bucket{tokens: min(r.burst, r.rpm), reset: now.Add(r.window)}
		b = r.buckets[key]
	}
	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// -------- Utility: GDPR-safe key extraction --------

type ProxyTrust struct {
	nets []*net.IPNet
}

func NewProxyTrust(cidrs []string) (*ProxyTrust, error) {
	var nets []*net.IPNet
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(strings.TrimSpace(c))
		if err != nil {
			return nil, err
		}
		nets = append(nets, n)
	}
	return &ProxyTrust{nets: nets}, nil
}

func (pt *ProxyTrust) isTrusted(ip net.IP) bool {
	for _, n := range pt.nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// isPrivateIP returns true if the IP is in RFC 1918 / RFC 6598 private ranges
// (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10) or loopback.
// These are always trusted as reverse proxy sources.
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() {
		return true
	}
	privateRanges := []struct {
		start net.IP
		end   net.IP
	}{
		{net.ParseIP("10.0.0.0"), net.ParseIP("10.255.255.255")},
		{net.ParseIP("172.16.0.0"), net.ParseIP("172.31.255.255")},
		{net.ParseIP("192.168.0.0"), net.ParseIP("192.168.255.255")},
		{net.ParseIP("100.64.0.0"), net.ParseIP("100.127.255.255")},
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	for _, r := range privateRanges {
		if bytes.Compare(ip4, r.start.To4()) >= 0 && bytes.Compare(ip4, r.end.To4()) <= 0 {
			return true
		}
	}
	return false
}

func getClientIP(r *http.Request, pt *ProxyTrust) net.IP {
	// If behind reverse proxy, trust X-Forwarded-For if remote is a configured
	// trusted proxy OR a private/RFC1918 IP (common Docker/K8s/reverse proxy setup).
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	remote := net.ParseIP(host)
	if remote == nil {
		return nil
	}

	trusted := (pt != nil && pt.isTrusted(remote)) || isPrivateIP(remote)
	if trusted {
		xff := r.Header.Get("X-Forwarded-For")
		if xff != "" {
			parts := strings.Split(xff, ",")
			ip := net.ParseIP(strings.TrimSpace(parts[0]))
			if ip != nil {
				return ip
			}
		}
	}
	return remote
}

// -------- Validation (strict allowlist) --------

var (
	// Allowed values for 'type' field
	allowedType = map[string]bool{"lxc": true, "vm": true, "pve": true, "addon": true, "tool": true}

	// Allowed values for 'status' field
	allowedStatus = map[string]bool{"installing": true, "validation": true, "configuring": true, "success": true, "failed": true, "aborted": true, "unknown": true}

	// Allowed values for 'os_type' field
	allowedOsType = map[string]bool{
		"debian": true, "ubuntu": true, "alpine": true, "devuan": true,
		"fedora": true, "rocky": true, "alma": true, "centos": true,
		"opensuse": true, "gentoo": true, "openeuler": true,
	}

	// Allowed values for 'gpu_vendor' field
	allowedGPUVendor = map[string]bool{"intel": true, "amd": true, "nvidia": true, "unknown": true, "": true}

	// Allowed values for 'gpu_passthrough' field
	allowedGPUPassthrough = map[string]bool{"igpu": true, "dgpu": true, "vgpu": true, "none": true, "unknown": true, "": true}

	// Allowed values for 'cpu_vendor' field
	allowedCPUVendor = map[string]bool{"intel": true, "amd": true, "arm": true, "apple": true, "qualcomm": true, "unknown": true, "": true}

	// Allowed values for 'error_category' field (must match PocketBase schema)
	allowedErrorCategory = map[string]bool{
		"network": true, "storage": true, "dependency": true, "permission": true,
		"timeout": true, "config": true, "resource": true, "unknown": true, "": true,
		"user_aborted": true, "apt": true, "command_not_found": true,
		"service": true, "database": true, "signal": true, "proxmox": true,
		"shell": true, "build": true,
	}

	// exitCodeInfo consolidates description and category for all known exit codes.
	// This is the single source of truth — dashboard.go and all other code should
	// use getExitCodeDescription() / getExitCodeCategory() instead of duplicating.
	exitCodeInfo = map[int]struct {
		Desc     string
		Category string
	}{
		// --- Generic / Shell ---
		0: {"Success", ""},
		1: {"General error", "unknown"},
		2: {"Misuse of shell builtins", "unknown"},
		3: {"General syntax or argument error", "unknown"},

		// --- curl / wget ---
		4:  {"curl: Feature not supported or protocol error", "network"},
		5:  {"curl: Could not resolve proxy", "network"},
		6:  {"curl: DNS resolution failed", "network"},
		7:  {"curl: Connection refused / host down", "network"},
		8:  {"curl: Server reply error", "network"},
		16: {"curl: HTTP/2 framing layer error", "network"},
		18: {"curl: Partial file (transfer incomplete)", "network"},
		22: {"curl: HTTP error (404/500 etc.)", "network"},
		23: {"curl: Write error (disk full?)", "storage"},
		24: {"curl: Write to local file failed", "storage"},
		25: {"curl: Upload failed", "network"},
		26: {"curl: Read error on local file (I/O)", "storage"},
		27: {"curl: Out of memory", "resource"},
		28: {"curl: Connection timed out", "timeout"},
		30: {"curl: FTP port command failed", "network"},
		32: {"curl: FTP SIZE command failed", "network"},
		33: {"curl: HTTP range error", "network"},
		34: {"curl: HTTP post error", "network"},
		35: {"curl: SSL/TLS handshake failed", "network"},
		36: {"curl: FTP bad download resume", "network"},
		47: {"curl: Too many redirects", "network"},
		51: {"curl: SSL peer certificate verification failed", "network"},
		52: {"curl: Empty reply from server", "network"},
		55: {"curl: Failed sending network data", "network"},
		56: {"curl: Receive error (connection reset)", "network"},
		59: {"curl: Couldn't use specified SSL cipher", "network"},
		75: {"Temporary failure (retry later)", "network"},
		78: {"curl: Remote file not found (404)", "network"},
		92: {"curl: HTTP/2 stream error", "network"},
		95: {"curl: HTTP/3 layer error", "network"},

		// --- Docker / Privileged ---
		10: {"Docker / privileged mode required", "config"},

		// --- BSD sysexits.h (64-78) ---
		64: {"Usage error (wrong arguments)", "config"},
		65: {"Data format error (bad input data)", "unknown"},
		66: {"Input file not found", "unknown"},
		67: {"User not found", "unknown"},
		68: {"Host not found", "network"},
		69: {"Service unavailable", "service"},
		70: {"Internal software error", "unknown"},
		71: {"System error (OS-level failure)", "unknown"},
		72: {"Critical OS file missing", "unknown"},
		73: {"Cannot create output file", "storage"},
		74: {"I/O error", "storage"},
		76: {"Remote protocol error", "network"},
		77: {"Permission denied", "permission"},

		// --- APT / DPKG ---
		100: {"APT: Package manager error (broken packages)", "apt"},
		101: {"APT: Configuration error (bad sources)", "apt"},
		102: {"APT: Lock held by another process", "apt"},

		// --- Script Validation & Setup (103-123) ---
		103: {"Validation: Shell is not Bash", "config"},
		104: {"Validation: Not running as root", "permission"},
		105: {"Validation: PVE version not supported", "config"},
		106: {"Validation: Architecture not supported (ARM/PiMox)", "config"},
		107: {"Validation: Kernel key parameters unreadable", "config"},
		108: {"Validation: Kernel key limits exceeded", "config"},
		109: {"Proxmox: No available container ID", "proxmox"},
		110: {"Proxmox: Failed to apply default.vars", "proxmox"},
		111: {"Proxmox: App defaults file not available", "proxmox"},
		112: {"Proxmox: Invalid install menu option", "config"},
		113: {"LXC: Under-provisioned — user aborted", "user_aborted"},
		114: {"LXC: Storage too low — user aborted", "user_aborted"},
		115: {"Download: install.func failed or incomplete", "network"},
		116: {"Proxmox: Default bridge vmbr0 not found", "config"},
		117: {"LXC: Container did not reach running state", "proxmox"},
		118: {"LXC: No IP assigned after timeout", "timeout"},
		119: {"Proxmox: No valid storage for rootdir", "storage"},
		120: {"Proxmox: No valid storage for vztmpl", "storage"},
		121: {"LXC: Container network not ready", "network"},
		122: {"LXC: No internet — user declined", "user_aborted"},
		123: {"LXC: Local IP detection failed", "network"},

		// --- Common shell/system errors ---
		124: {"Command timed out", "timeout"},
		125: {"Docker daemon error / command failed to start", "config"},
		126: {"Command cannot execute (permission problem)", "permission"},
		127: {"Command not found", "command_not_found"},
		128: {"Invalid argument to exit", "signal"},
		129: {"Killed by SIGHUP (terminal closed)", "user_aborted"},
		130: {"Script terminated by Ctrl+C (SIGINT)", "user_aborted"},
		131: {"Killed by SIGQUIT (core dump)", "signal"},
		132: {"Killed by SIGILL (illegal instruction)", "signal"},
		134: {"Process aborted (SIGABRT)", "signal"},
		137: {"Process killed (SIGKILL) — likely OOM", "resource"},
		139: {"Segmentation fault (SIGSEGV)", "unknown"},
		141: {"Broken pipe (SIGPIPE)", "signal"},
		143: {"Process terminated (SIGTERM)", "signal"},
		144: {"Killed by signal 16 (SIGUSR1/SIGSTKFLT)", "signal"},
		146: {"Killed by signal 18 (SIGTSTP)", "signal"},

		// --- Systemd / Service errors (150-154) ---
		150: {"Systemd: Service failed to start", "service"},
		151: {"Systemd: Service unit not found", "service"},
		152: {"Permission denied (EACCES)", "permission"},
		153: {"Build/compile failed (make/gcc/cmake)", "build"},
		154: {"Node.js: Native addon build failed (node-gyp)", "build"},

		// --- Python / pip / uv (160-162) ---
		160: {"Python: Virtualenv/uv environment missing or broken", "dependency"},
		161: {"Python: Dependency resolution failed", "dependency"},
		162: {"Python: Installation aborted (EXTERNALLY-MANAGED)", "dependency"},

		// --- PostgreSQL (170-173) ---
		170: {"PostgreSQL: Connection failed", "database"},
		171: {"PostgreSQL: Authentication failed", "database"},
		172: {"PostgreSQL: Database does not exist", "database"},
		173: {"PostgreSQL: Fatal error in query", "database"},

		// --- MySQL / MariaDB (180-183) ---
		180: {"MySQL/MariaDB: Connection failed", "database"},
		181: {"MySQL/MariaDB: Authentication failed", "database"},
		182: {"MySQL/MariaDB: Database does not exist", "database"},
		183: {"MySQL/MariaDB: Fatal error in query", "database"},

		// --- MongoDB (190-193) ---
		190: {"MongoDB: Connection failed", "database"},
		191: {"MongoDB: Authentication failed", "database"},
		192: {"MongoDB: Database not found", "database"},
		193: {"MongoDB: Fatal query error", "database"},

		// --- Proxmox Custom Codes (200-231) ---
		200: {"Proxmox: Failed to create lock file", "proxmox"},
		203: {"Proxmox: Missing CTID variable", "config"},
		204: {"Proxmox: Missing PCT_OSTYPE variable", "config"},
		205: {"Proxmox: Invalid CTID (<100)", "config"},
		206: {"Proxmox: CTID already in use", "config"},
		207: {"Proxmox: Password contains unescaped special chars", "config"},
		208: {"Proxmox: Invalid configuration (DNS/MAC/Network)", "config"},
		209: {"Proxmox: Container creation failed", "proxmox"},
		210: {"Proxmox: Cluster not quorate", "proxmox"},
		211: {"Proxmox: Timeout waiting for template lock", "timeout"},
		212: {"Proxmox: Storage 'iscsidirect' does not support containers", "proxmox"},
		213: {"Proxmox: Storage does not support 'rootdir' content", "proxmox"},
		214: {"Proxmox: Not enough storage space", "storage"},
		215: {"Proxmox: Container created but not listed (ghost state)", "proxmox"},
		216: {"Proxmox: RootFS entry missing in config", "proxmox"},
		217: {"Proxmox: Storage not accessible", "storage"},
		218: {"Proxmox: Template file corrupted or incomplete", "proxmox"},
		219: {"Proxmox: CephFS does not support containers", "storage"},
		220: {"Proxmox: Unable to resolve template path", "proxmox"},
		221: {"Proxmox: Template file not readable", "proxmox"},
		222: {"Proxmox: Template download failed", "proxmox"},
		223: {"Proxmox: Template not available after download", "proxmox"},
		224: {"Proxmox: PBS storage is for backups only", "storage"},
		225: {"Proxmox: No template available for OS/Version", "proxmox"},
		226: {"Proxmox: VM disk import or post-creation setup failed", "proxmox"},
		231: {"Proxmox: LXC stack upgrade failed", "proxmox"},

		// --- Tools & Addon Scripts (232-238) ---
		232: {"Tools: Wrong execution environment", "config"},
		233: {"Tools: Application not installed (update prerequisite missing)", "config"},
		234: {"Tools: No LXC containers found", "proxmox"},
		235: {"Tools: Backup or restore operation failed", "storage"},
		236: {"Tools: Required hardware not detected", "config"},
		237: {"Tools: Dependency package installation failed", "dependency"},
		238: {"Tools: OS or distribution not supported", "config"},

		// --- Node.js / npm (239-249) ---
		239: {"npm/Node.js: Unexpected runtime error", "dependency"},
		243: {"Node.js: Out of memory (heap overflow)", "resource"},
		245: {"Node.js: Invalid command-line option", "config"},
		246: {"Node.js: Internal JavaScript Parse Error", "unknown"},
		247: {"Node.js: Fatal internal error", "unknown"},
		248: {"Node.js: Invalid C++ addon / N-API failure", "unknown"},
		249: {"npm/pnpm/yarn: Unknown fatal error", "unknown"},

		// --- Application Install/Update Errors (250-254) ---
		250: {"App: Download failed or version not determined", "network"},
		251: {"App: File extraction failed (corrupt/incomplete)", "storage"},
		252: {"App: Required file or resource not found", "unknown"},
		253: {"App: Data migration required — update aborted", "config"},
		254: {"App: User declined prompt or input timed out", "user_aborted"},

		// --- DPKG ---
		255: {"DPKG: Fatal internal error / set -e triggered", "apt"},
	}
)

// getExitCodeDescription returns the human-readable description for an exit code.
// Falls back to signal-based description for codes 128-191, or "Unknown" otherwise.
func getExitCodeDescription(code int) string {
	if info, ok := exitCodeInfo[code]; ok {
		return info.Desc
	}
	if code > 128 && code < 192 {
		sigNum := code - 128
		sigNames := map[int]string{
			1: "SIGHUP", 2: "SIGINT", 3: "SIGQUIT", 6: "SIGABRT",
			9: "SIGKILL", 11: "SIGSEGV", 13: "SIGPIPE", 15: "SIGTERM",
			24: "SIGXCPU", 25: "SIGXFSZ",
		}
		if name, ok := sigNames[sigNum]; ok {
			return fmt.Sprintf("Killed by %s (signal %d)", name, sigNum)
		}
		return fmt.Sprintf("Killed by signal %d", sigNum)
	}
	return fmt.Sprintf("Unknown (exit code %d)", code)
}

// getExitCodeCategory returns the error category for an exit code.
// Falls back to "signal" for codes 128-191, or "unknown" otherwise.
func getExitCodeCategory(code int) string {
	if info, ok := exitCodeInfo[code]; ok {
		return info.Category
	}
	if code > 128 && code < 192 {
		return "signal"
	}
	return "unknown"
}

func sanitizeShort(s string, max int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// remove line breaks and high-risk chars
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	if len(s) > max {
		s = s[:max]
	}
	return s
}

// sanitizeMultiLine allows newlines (for log output) but limits total length.
func sanitizeMultiLine(s string, max int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	if len(s) > max {
		s = s[:max]
	}
	return s
}

// ipv4Re matches IPv4 addresses (e.g. 192.168.1.100) for GDPR anonymization.
var ipv4Re = regexp.MustCompile(`(\d{1,3}\.)\d{1,3}\.\d{1,3}`)

// sanitizeIPs anonymizes IPv4 addresses in log text for GDPR compliance.
// Keeps the first octet visible for debugging (e.g. "192.x.x"), strips the rest.
func sanitizeIPs(s string) string {
	if s == "" {
		return s
	}
	return ipv4Re.ReplaceAllString(s, "${1}x.x")
}

func validate(in *TelemetryIn) error {
	// Sanitize all string fields
	in.RandomID = sanitizeShort(in.RandomID, 64)
	in.ExecutionID = sanitizeShort(in.ExecutionID, 64)
	in.Type = sanitizeShort(in.Type, 8)
	in.NSAPP = sanitizeShort(in.NSAPP, 64)
	in.Status = sanitizeShort(in.Status, 16)
	in.OsType = sanitizeShort(in.OsType, 32)
	in.OsVersion = sanitizeShort(in.OsVersion, 32)
	in.PveVer = sanitizeShort(in.PveVer, 32)
	in.Method = sanitizeShort(in.Method, 32)

	// Sanitize extended fields
	in.GPUVendor = strings.ToLower(sanitizeShort(in.GPUVendor, 16))
	in.GPUModel = sanitizeShort(in.GPUModel, 64)
	in.GPUPassthrough = strings.ToLower(sanitizeShort(in.GPUPassthrough, 16))
	in.CPUVendor = strings.ToLower(sanitizeShort(in.CPUVendor, 16))
	in.CPUModel = sanitizeShort(in.CPUModel, 64)
	in.RAMSpeed = sanitizeShort(in.RAMSpeed, 16)
	in.ErrorCategory = strings.ToLower(sanitizeShort(in.ErrorCategory, 32))

	// Sanitize repo_source (routing field)
	in.RepoSource = sanitizeShort(in.RepoSource, 64)

	// Default empty values to "unknown" for consistency
	if in.GPUVendor == "" {
		in.GPUVendor = "unknown"
	}
	if in.GPUPassthrough == "" {
		in.GPUPassthrough = "unknown"
	}
	if in.CPUVendor == "" {
		in.CPUVendor = "unknown"
	}

	// Allow longer error text to capture full installation log + anonymize IPs (GDPR)
	in.Error = sanitizeIPs(sanitizeMultiLine(in.Error, 131072))

	// Required fields for all requests
	if in.RandomID == "" || in.Type == "" || in.NSAPP == "" || in.Status == "" {
		return errors.New("missing required fields: random_id, type, nsapp, status")
	}

	// Normalize common typos for backwards compatibility
	if in.Status == "sucess" {
		in.Status = "success"
	}

	// Validate enums with fallback to safe defaults (never reject writes)
	if !allowedType[in.Type] {
		log.Printf("[WARN] unknown type %q from nsapp=%s, rejecting", in.Type, in.NSAPP)
		return errors.New("invalid type")
	}
	if !allowedStatus[in.Status] {
		log.Printf("[WARN] unknown status %q from nsapp=%s, falling back to 'unknown'", in.Status, in.NSAPP)
		in.Status = "unknown"
	}

	// Enum fields: fallback to "unknown" instead of rejecting
	if !allowedGPUVendor[in.GPUVendor] {
		log.Printf("[WARN] unknown gpu_vendor %q from nsapp=%s, falling back to 'unknown'", in.GPUVendor, in.NSAPP)
		in.GPUVendor = "unknown"
	}
	if !allowedGPUPassthrough[in.GPUPassthrough] {
		log.Printf("[WARN] unknown gpu_passthrough %q from nsapp=%s, falling back to 'unknown'", in.GPUPassthrough, in.NSAPP)
		in.GPUPassthrough = "unknown"
	}
	if !allowedCPUVendor[in.CPUVendor] {
		log.Printf("[WARN] unknown cpu_vendor %q from nsapp=%s, falling back to 'unknown'", in.CPUVendor, in.NSAPP)
		in.CPUVendor = "unknown"
	}
	if !allowedErrorCategory[in.ErrorCategory] {
		log.Printf("[WARN] unknown error_category %q from nsapp=%s, falling back to 'unknown'", in.ErrorCategory, in.NSAPP)
		in.ErrorCategory = "unknown"
	}

	// For status updates (not installing), skip numeric field validation
	// These are only required for initial creation
	isUpdate := in.Status != "installing"

	// os_type is optional but if provided must be valid (only for lxc/vm)
	if (in.Type == "lxc" || in.Type == "vm") && in.OsType != "" && !allowedOsType[in.OsType] {
		return errors.New("invalid os_type")
	}

	// method is optional and flexible - just sanitized, no strict validation
	// Values like "default", "advanced", "mydefaults-global", "mydefaults-app" are all valid

	// Validate numeric ranges (only strict for new records)
	if !isUpdate && (in.Type == "lxc" || in.Type == "vm") {
		if in.CTType < 0 || in.CTType > 2 {
			return errors.New("invalid ct_type (must be 0, 1, or 2)")
		}
	}
	if in.DiskSize < 0 || in.DiskSize > 100000 {
		return errors.New("invalid disk_size")
	}
	if in.CoreCount < 0 || in.CoreCount > 256 {
		return errors.New("invalid core_count")
	}
	if in.RAMSize < 0 || in.RAMSize > 1048576 {
		return errors.New("invalid ram_size")
	}
	if in.ExitCode < 0 || in.ExitCode > 255 {
		return errors.New("invalid exit_code")
	}
	if in.InstallDuration < 0 || in.InstallDuration > 86400 {
		return errors.New("invalid install_duration (max 24h)")
	}

	// Validate repo_source: must be a known value or empty
	if in.RepoSource != "" && !allowedRepoSource[in.RepoSource] {
		return fmt.Errorf("rejected repo_source '%s' (must be 'ProxmoxVE', 'ProxmoxVED', or 'external')", in.RepoSource)
	}

	return nil
}

// computeHash generates a hash for deduplication (GDPR-safe, no IP)
func computeHash(out TelemetryOut) string {
	key := fmt.Sprintf("%s|%s|%s|%s|%d",
		out.RandomID, out.NSAPP, out.Type, out.Status, out.ExitCode,
	)
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// categorizeErrorText assigns an error_category based on error text patterns
func categorizeErrorText(errLower string) string {
	// Docker / container errors (check early, before generic patterns)
	if strings.Contains(errLower, "docker") ||
		strings.Contains(errLower, "privileged mode") ||
		strings.Contains(errLower, "container runtime") ||
		strings.Contains(errLower, "daemon") {
		return "config"
	}
	// Network errors
	if strings.Contains(errLower, "connection refused") ||
		strings.Contains(errLower, "could not resolve") ||
		strings.Contains(errLower, "dns") ||
		strings.Contains(errLower, "failed to download") ||
		strings.Contains(errLower, "curl") ||
		strings.Contains(errLower, "wget") ||
		strings.Contains(errLower, "network unreachable") ||
		strings.Contains(errLower, "timed out") ||
		strings.Contains(errLower, "ssl") ||
		strings.Contains(errLower, "certificate") {
		return "network"
	}
	// APT / package manager (check before generic "dependency")
	if strings.Contains(errLower, "apt") ||
		strings.Contains(errLower, "dpkg") ||
		strings.Contains(errLower, "broken packages") ||
		strings.Contains(errLower, "unmet dependencies") ||
		strings.Contains(errLower, "unable to locate package") {
		return "apt"
	}
	// Storage
	if strings.Contains(errLower, "no space left") ||
		strings.Contains(errLower, "disk full") ||
		strings.Contains(errLower, "read-only file system") ||
		strings.Contains(errLower, "i/o error") {
		return "storage"
	}
	// Permission
	if strings.Contains(errLower, "permission denied") ||
		strings.Contains(errLower, "operation not permitted") ||
		strings.Contains(errLower, "access denied") {
		return "permission"
	}
	// Resource (OOM, memory)
	if strings.Contains(errLower, "oom") ||
		strings.Contains(errLower, "out of memory") ||
		strings.Contains(errLower, "cannot allocate") ||
		strings.Contains(errLower, "killed") ||
		strings.Contains(errLower, "sigkill") {
		return "resource"
	}
	// Signal-related errors
	if strings.Contains(errLower, "sighup") ||
		strings.Contains(errLower, "sigquit") ||
		strings.Contains(errLower, "sigterm") ||
		strings.Contains(errLower, "sigabrt") ||
		strings.Contains(errLower, "sigpipe") ||
		strings.Contains(errLower, "core dump") {
		return "signal"
	}
	// Command not found
	if strings.Contains(errLower, "command not found") ||
		strings.Contains(errLower, "not found") {
		return "command_not_found"
	}
	// Dependency
	if strings.Contains(errLower, "dependency") ||
		strings.Contains(errLower, "requires") ||
		strings.Contains(errLower, "missing") {
		return "dependency"
	}
	// Config
	if strings.Contains(errLower, "config") ||
		strings.Contains(errLower, "syntax error") ||
		strings.Contains(errLower, "invalid") {
		return "config"
	}
	// Timeout
	if strings.Contains(errLower, "timeout") {
		return "timeout"
	}
	return "unknown"
}

// -------- HTTP server --------

func serveHTMLFile(w http.ResponseWriter, r *http.Request, filePath string) {
	content, err := publicFS.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		log.Printf("Error reading embedded file %s: %v", filePath, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	_, _ = w.Write(content)
}

func main() {
	cfg := Config{
		ListenAddr:         env("LISTEN_ADDR", ":8080"),
		TrustedProxiesCIDR: splitCSV(env("TRUSTED_PROXIES_CIDR", "")),

		PBBaseURL:        mustEnv("PB_URL"),
		PBAuthCollection: mustEnv("PB_AUTH_COLLECTION"),
		PBIdentity:       mustEnv("PB_IDENTITY"),
		PBPassword:       mustEnv("PB_PASSWORD"),
		PBTargetColl:     mustEnv("PB_TARGET_COLLECTION"),

		MaxBodyBytes:     envInt64("MAX_BODY_BYTES", 262144),
		RateLimitRPM:     envInt("RATE_LIMIT_RPM", 300),
		RateBurst:        envInt("RATE_BURST", 60),
		RateKeyMode:      env("RATE_KEY_MODE", "ip"), // "ip" or "header"
		RateKeyHeader:    env("RATE_KEY_HEADER", "X-Telemetry-Key"),
		RequestTimeout:   time.Duration(envInt("UPSTREAM_TIMEOUT_MS", 60000)) * time.Millisecond,
		EnableReqLogging: envBool("ENABLE_REQUEST_LOGGING", false),

		// Cache config
		RedisURL:     env("REDIS_URL", ""),
		EnableRedis:  envBool("ENABLE_REDIS", false),
		CacheTTL:     time.Duration(envInt("CACHE_TTL_SECONDS", 3600)) * time.Second,
		CacheEnabled: envBool("ENABLE_CACHE", true),

		// Alert config
		AlertEnabled:          envBool("ALERT_ENABLED", false),
		SMTPHost:              env("SMTP_HOST", ""),
		SMTPPort:              envInt("SMTP_PORT", 587),
		SMTPUser:              env("SMTP_USER", ""),
		SMTPPassword:          env("SMTP_PASSWORD", ""),
		SMTPFrom:              env("SMTP_FROM", "telemetry@proxmoxved.local"),
		SMTPTo:                splitCSV(env("SMTP_TO", "")),
		SMTPUseTLS:            envBool("SMTP_USE_TLS", false),
		AlertFailureThreshold: envFloat("ALERT_FAILURE_THRESHOLD", 20.0),
		AlertCheckInterval:    time.Duration(envInt("ALERT_CHECK_INTERVAL_MIN", 15)) * time.Minute,
		AlertCooldown:         time.Duration(envInt("ALERT_COOLDOWN_MIN", 60)) * time.Minute,

		// GitHub integration
		GitHubToken:   env("GITHUB_TOKEN", ""),
		GitHubOwner:   env("GITHUB_OWNER", "community-scripts"),
		GitHubRepo:    env("GITHUB_REPO", "ProxmoxVE"),
		AdminPassword: env("ADMIN_PASSWORD", ""),
	}

	// Debug: log whether critical env vars are set (not the values!)
	log.Printf("CONFIG: ADMIN_PASSWORD set=%v (len=%d), GITHUB_TOKEN set=%v, GITHUB_OWNER=%s, GITHUB_REPO=%s",
		cfg.AdminPassword != "", len(cfg.AdminPassword), cfg.GitHubToken != "", cfg.GitHubOwner, cfg.GitHubRepo)

	var pt *ProxyTrust
	if strings.TrimSpace(env("TRUSTED_PROXIES_CIDR", "")) != "" {
		p, err := NewProxyTrust(cfg.TrustedProxiesCIDR)
		if err != nil {
			log.Fatalf("invalid TRUSTED_PROXIES_CIDR: %v", err)
		}
		pt = p
	}

	pb := NewPBClient(cfg)

	// Auto-migrate: ensure 'pipeline' field exists in PocketBase collection.
	// Retry up to 3 times with backoff in case PB is still starting up.
	{
		var migErr error
		for attempt := 1; attempt <= 3; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			migErr = pb.EnsurePipelineField(ctx)
			cancel()
			if migErr == nil {
				break
			}
			log.Printf("[MIGRATION] Attempt %d/3 failed: %v", attempt, migErr)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt*2) * time.Second)
			}
		}
		if migErr != nil {
			log.Printf("[MIGRATION] WARNING: pipeline field migration failed after 3 attempts: %v", migErr)
			log.Printf("[MIGRATION] Pipeline tracking will NOT work until the field is added manually or the service is restarted")
		}
	}

	// Persistent script stats stores (7d, 30d, all-time)
	stats7d := NewScriptStatsStore(pb, "_script_stats_7d", 7)
	stats30d := NewScriptStatsStore(pb, "_script_stats_30d", 30)
	statsAllTime := NewScriptStatsStore(pb, "_script_stats_alltime", 0)
	scriptStores := []*ScriptStatsStore{stats7d, stats30d, statsAllTime}

	rl := NewRateLimiter(cfg.RateLimitRPM, cfg.RateBurst)

	// Initialize cache
	cache := NewCache(CacheConfig{
		RedisURL:    cfg.RedisURL,
		EnableRedis: cfg.EnableRedis,
		DefaultTTL:  cfg.CacheTTL,
	})

	// Initialize alerter
	alerter := NewAlerter(AlertConfig{
		Enabled:          cfg.AlertEnabled,
		SMTPHost:         cfg.SMTPHost,
		SMTPPort:         cfg.SMTPPort,
		SMTPUser:         cfg.SMTPUser,
		SMTPPassword:     cfg.SMTPPassword,
		SMTPFrom:         cfg.SMTPFrom,
		SMTPTo:           cfg.SMTPTo,
		UseTLS:           cfg.SMTPUseTLS,
		FailureThreshold: cfg.AlertFailureThreshold,
		CheckInterval:    cfg.AlertCheckInterval,
		Cooldown:         cfg.AlertCooldown,
	}, pb)
	alerter.Start()

	// Initialize cleanup/retention job (GDPR Löschkonzept)
	cleaner := NewCleaner(CleanupConfig{
		Enabled:          envBool("CLEANUP_ENABLED", true),
		CheckInterval:    time.Duration(envInt("CLEANUP_INTERVAL_MIN", 15)) * time.Minute,
		StuckAfterHours:  envInt("CLEANUP_STUCK_HOURS", 1),
		RetentionEnabled: envBool("RETENTION_ENABLED", false),
		RetentionDays:    envInt("RETENTION_DAYS", 365),
	}, pb)
	cleaner.Start()

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Check PocketBase connectivity
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status := map[string]interface{}{
			"status": "ok",
			"time":   time.Now().UTC().Format(time.RFC3339),
		}

		if err := pb.ensureAuth(ctx); err != nil {
			status["status"] = "degraded"
			status["pocketbase"] = "disconnected"
			w.WriteHeader(503)
		} else {
			status["pocketbase"] = "connected"
			w.WriteHeader(200)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// Dashboard HTML page - serve on root
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		serveHTMLFile(w, r, "public/templates/dashboard.html")
	})

	// Redirect /dashboard to / for backwards compatibility
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		serveHTMLFile(w, r, "public/templates/dashboard.html")
	})

	// Prometheus-style metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		data, err := pb.FetchDashboardData(ctx, 1, "ProxmoxVE") // Last 24h, production only for metrics
		if err != nil {
			http.Error(w, "failed to fetch metrics", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP telemetry_installs_total Total number of installations\n")
		fmt.Fprintf(w, "# TYPE telemetry_installs_total counter\n")
		fmt.Fprintf(w, "telemetry_installs_total %d\n\n", data.TotalInstalls)
		fmt.Fprintf(w, "# HELP telemetry_installs_success_total Successful installations\n")
		fmt.Fprintf(w, "# TYPE telemetry_installs_success_total counter\n")
		fmt.Fprintf(w, "telemetry_installs_success_total %d\n\n", data.SuccessCount)
		fmt.Fprintf(w, "# HELP telemetry_installs_failed_total Failed installations\n")
		fmt.Fprintf(w, "# TYPE telemetry_installs_failed_total counter\n")
		fmt.Fprintf(w, "telemetry_installs_failed_total %d\n\n", data.FailedCount)
		fmt.Fprintf(w, "# HELP telemetry_installs_aborted_total Aborted installations (SIGINT)\n")
		fmt.Fprintf(w, "# TYPE telemetry_installs_aborted_total counter\n")
		fmt.Fprintf(w, "telemetry_installs_aborted_total %d\n\n", data.AbortedCount)
		fmt.Fprintf(w, "# HELP telemetry_installs_pending Current installing count\n")
		fmt.Fprintf(w, "# TYPE telemetry_installs_pending gauge\n")
		fmt.Fprintf(w, "telemetry_installs_pending %d\n\n", data.InstallingCount)
		fmt.Fprintf(w, "# HELP telemetry_success_rate Success rate percentage\n")
		fmt.Fprintf(w, "# TYPE telemetry_success_rate gauge\n")
		fmt.Fprintf(w, "telemetry_success_rate %.2f\n", data.SuccessRate)
	})

	// Dashboard API endpoint (with caching)
	mux.HandleFunc("/api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		days := 1 // Default: Today
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
			if days < 0 {
				days = 1
			}
			// Cap to 365 days to prevent unbounded queries that timeout
			if days == 0 || days > 365 {
				days = 365
			}
		}

		// repo_source filter (default: ProxmoxVE)
		repoSource := r.URL.Query().Get("repo")
		if repoSource == "" {
			repoSource = "ProxmoxVE"
		}
		// "all" means no filter
		if repoSource == "all" {
			repoSource = ""
		}

		// Increase timeout for large datasets (dashboard aggregation takes time)
		ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
		defer cancel()

		// Try cache first (stale-while-revalidate)
		cacheKey := fmt.Sprintf("dashboard:%d:%s", days, repoSource)
		var data *DashboardData
		if cfg.CacheEnabled && cache.Get(ctx, cacheKey, &data) {
			// Serve cached data immediately
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")

			// If stale, trigger background refresh (non-blocking)
			if cache.IsStale(ctx, cacheKey) {
				w.Header().Set("X-Cache", "STALE")
				if cache.TryStartRefresh(cacheKey) {
					go func() {
						defer cache.FinishRefresh(cacheKey)
						refreshCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
						defer cancel()
						freshData, err := pb.FetchDashboardData(refreshCtx, days, repoSource)
						if err != nil {
							log.Printf("[CACHE] background refresh failed for %s: %v", cacheKey, err)
							return
						}
						_ = cache.Set(context.Background(), cacheKey, freshData, cfg.CacheTTL)
						log.Printf("[CACHE] background refresh completed for %s", cacheKey)
					}()
				}
			}

			json.NewEncoder(w).Encode(data)
			return
		}

		data, err := pb.FetchDashboardData(ctx, days, repoSource)
		if err != nil {
			log.Printf("dashboard fetch failed: %v", err)
			http.Error(w, "failed to fetch data", http.StatusInternalServerError)
			return
		}

		// Cache the result with dynamic TTL based on period
		if cfg.CacheEnabled {
			// Short periods change faster → shorter cache TTL
			cacheTTL := cfg.CacheTTL
			switch {
			case days <= 1:
				cacheTTL = 30 * time.Second // Today: 30s cache
			case days <= 7:
				cacheTTL = 2 * time.Minute // 7 Days: 2min cache
			case days <= 30:
				cacheTTL = 5 * time.Minute // 30 Days: 5min cache
			case days <= 90:
				cacheTTL = 15 * time.Minute // 90 Days: 15min cache
			default:
				cacheTTL = 30 * time.Minute // 1 Year+: 30min cache
			}
			_ = cache.Set(ctx, cacheKey, data, cacheTTL)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		json.NewEncoder(w).Encode(data)
	})

	// Paginated records API
	mux.HandleFunc("/api/records", func(w http.ResponseWriter, r *http.Request) {
		page := 1
		limit := 50
		status := r.URL.Query().Get("status")
		app := r.URL.Query().Get("app")
		osType := r.URL.Query().Get("os")
		typeFilter := r.URL.Query().Get("type")
		sort := r.URL.Query().Get("sort")
		repoSource := r.URL.Query().Get("repo")
		if repoSource == "" {
			repoSource = "ProxmoxVE" // Default filter: production data
		}
		if repoSource == "all" {
			repoSource = ""
		}

		// Days filter for Installation Log (default: 1 = today)
		days := 1
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
			if days < 0 {
				days = 1
			}
			if days == 0 || days > 365 {
				days = 365
			}
		}

		if p := r.URL.Query().Get("page"); p != "" {
			fmt.Sscanf(p, "%d", &page)
			if page < 1 {
				page = 1
			}
		}
		if l := r.URL.Query().Get("limit"); l != "" {
			fmt.Sscanf(l, "%d", &limit)
			if limit < 1 {
				limit = 1
			}
			if limit > 100 {
				limit = 100
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		records, total, err := pb.FetchRecordsPaginated(ctx, page, limit, status, app, osType, typeFilter, sort, repoSource, days)
		if err != nil {
			log.Printf("records fetch failed: %v", err)
			http.Error(w, "failed to fetch records", http.StatusInternalServerError)
			return
		}

		// Auto-reclassify old records that have status=failed but are actually SIGINT aborts
		for i := range records {
			if records[i].Status == "failed" && (records[i].ExitCode == 130 ||
				strings.Contains(strings.ToLower(records[i].Error), "sigint") ||
				strings.Contains(strings.ToLower(records[i].Error), "ctrl+c") ||
				strings.Contains(strings.ToLower(records[i].Error), "ctrl-c")) {
				records[i].Status = "aborted"
			}
		}

		response := map[string]interface{}{
			"records":     records,
			"page":        page,
			"limit":       limit,
			"total":       total,
			"total_pages": (total + limit - 1) / limit,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// Alert history and test endpoints
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"enabled": cfg.AlertEnabled,
			"history": alerter.GetAlertHistory(),
		})
	})

	mux.HandleFunc("/api/alerts/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := alerter.TestAlert(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test alert sent"))
	})

	// Error Analysis page
	mux.HandleFunc("/error-analysis", func(w http.ResponseWriter, r *http.Request) {
		serveHTMLFile(w, r, "public/templates/error-analysis.html")
	})

	// Script Analysis page
	mux.HandleFunc("/script-analysis", func(w http.ResponseWriter, r *http.Request) {
		serveHTMLFile(w, r, "public/templates/script-analysis.html")
	})

	// Script Analysis API
	mux.HandleFunc("/api/scripts", func(w http.ResponseWriter, r *http.Request) {
		days := 30
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
			if days < 0 {
				days = 1
			}
			// days=0 is allowed → All Time (served from AllTimeStore)
			if days > 365 && days != 0 {
				days = 365
			}
		}

		repoSource := r.URL.Query().Get("repo")
		if repoSource == "" {
			repoSource = "ProxmoxVE"
		}
		if repoSource == "all" {
			repoSource = ""
		}

		ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
		defer cancel()

		// Persistent stores: 7d, 30d, All Time (days=0)
		// Only Today (days=1) uses live telemetry fetch
		var store *ScriptStatsStore
		switch {
		case days == 0:
			store = statsAllTime
		case days <= 7:
			store = stats7d
		case days <= 30:
			store = stats30d
		}

		if store != nil {
			cacheKey := fmt.Sprintf("scripts:%d:%s", days, repoSource)
			var data *ScriptAnalysisData
			if cfg.CacheEnabled && cache.Get(ctx, cacheKey, &data) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				json.NewEncoder(w).Encode(data)
				return
			}

			// Build from persistent store (instant, ~466 rows in memory)
			knownScripts, _ := pb.FetchKnownScripts(ctx)
			data = store.BuildData(knownScripts)

			if cfg.CacheEnabled {
				_ = cache.Set(ctx, cacheKey, data, 23*time.Hour)
			}

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "MISS")
			json.NewEncoder(w).Encode(data)
			return
		}

		// Today (days=1): live fetch from telemetry
		cacheKey := fmt.Sprintf("scripts:%d:%s", days, repoSource)
		var data *ScriptAnalysisData
		if cfg.CacheEnabled && cache.Get(ctx, cacheKey, &data) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if cache.IsStale(ctx, cacheKey) {
				w.Header().Set("X-Cache", "STALE")
				if cache.TryStartRefresh(cacheKey) {
					go func() {
						defer cache.FinishRefresh(cacheKey)
						refreshCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
						defer cancel()
						freshData, err := pb.FetchScriptAnalysisData(refreshCtx, days, repoSource)
						if err != nil {
							log.Printf("[CACHE] background refresh failed for %s: %v", cacheKey, err)
							return
						}
						_ = cache.Set(context.Background(), cacheKey, freshData, 2*time.Minute)
					}()
				}
			}
			json.NewEncoder(w).Encode(data)
			return
		}

		data, err := pb.FetchScriptAnalysisData(ctx, days, repoSource)
		if err != nil {
			log.Printf("script analysis fetch failed: %v", err)
			http.Error(w, "failed to fetch script data", http.StatusInternalServerError)
			return
		}

		if cfg.CacheEnabled {
			_ = cache.Set(ctx, cacheKey, data, 2*time.Minute)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		json.NewEncoder(w).Encode(data)
	})

	// Error Analysis API - detailed error data
	mux.HandleFunc("/api/errors", func(w http.ResponseWriter, r *http.Request) {
		days := 7
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
			if days < 1 {
				days = 1
			}
			if days > 365 {
				days = 365
			}
		}

		repoSource := r.URL.Query().Get("repo")
		if repoSource == "" {
			repoSource = "ProxmoxVE"
		}
		if repoSource == "all" {
			repoSource = ""
		}

		// Scale timeout by data volume
		timeout := 120 * time.Second
		if days >= 90 {
			timeout = 300 * time.Second
		}
		if days >= 365 {
			timeout = 600 * time.Second
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		cacheKey := fmt.Sprintf("errors:%d:%s", days, repoSource)
		var data *ErrorAnalysisData
		if cfg.CacheEnabled && cache.Get(ctx, cacheKey, &data) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if cache.IsStale(ctx, cacheKey) {
				w.Header().Set("X-Cache", "STALE")
				if cache.TryStartRefresh(cacheKey) {
					go func() {
						defer cache.FinishRefresh(cacheKey)
						refreshTimeout := 120 * time.Second
						if days >= 90 {
							refreshTimeout = 300 * time.Second
						}
						if days >= 365 {
							refreshTimeout = 600 * time.Second
						}
						refreshCtx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
						defer cancel()
						freshData, err := pb.FetchErrorAnalysisData(refreshCtx, days, repoSource)
						if err != nil {
							log.Printf("[CACHE] background refresh failed for %s: %v", cacheKey, err)
							return
						}
						refreshTTL := 2 * time.Minute
						if days > 7 {
							refreshTTL = 23 * time.Hour
						}
						_ = cache.Set(context.Background(), cacheKey, freshData, refreshTTL)
					}()
				}
			}
			json.NewEncoder(w).Encode(data)
			return
		}

		data, err := pb.FetchErrorAnalysisData(ctx, days, repoSource)
		if err != nil {
			log.Printf("error analysis fetch failed: %v", err)
			http.Error(w, "failed to fetch error data", http.StatusInternalServerError)
			return
		}

		if cfg.CacheEnabled {
			cacheTTL := 2 * time.Minute
			if days > 7 {
				cacheTTL = 23 * time.Hour
			}
			_ = cache.Set(ctx, cacheKey, data, cacheTTL)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		json.NewEncoder(w).Encode(data)
	})

	// API: Get exit code descriptions (static reference data)
	mux.HandleFunc("/api/exit-codes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Build a simple map[int]string from the unified exitCodeInfo
		descs := make(map[int]string, len(exitCodeInfo))
		for code, info := range exitCodeInfo {
			descs[code] = info.Desc
		}
		json.NewEncoder(w).Encode(descs)
	})

	// Serve static files from the /public/static directory
	// Serve embedded static files
	staticFS, err := fs.Sub(publicFS, "public/static")
	if err != nil {
		log.Fatalf("Failed to create static FS: %v", err)
	}
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// Cleanup trigger & status API
	mux.HandleFunc("/api/cleanup/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		count, err := cleaner.GetStuckCount(ctx)
		if err != nil {
			http.Error(w, "failed to check stuck count", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"stuck_count":       count,
			"stuck_after_hours": cleaner.cfg.StuckAfterHours,
			"check_interval":    cleaner.cfg.CheckInterval.String(),
			"enabled":           cleaner.cfg.Enabled,
		})
	})

	mux.HandleFunc("/api/cleanup/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Require admin password
		if cfg.AdminPassword == "" {
			http.Error(w, "admin password not configured", http.StatusServiceUnavailable)
			return
		}
		password := r.Header.Get("X-Admin-Password")
		if password == "" {
			// Try from JSON body
			var body struct {
				Password string `json:"password"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			password = body.Password
		}
		if password != cfg.AdminPassword {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		updated, err := cleaner.RunNow()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"updated": updated,
			"message": fmt.Sprintf("Cleaned up %d stuck installations", updated),
		})
	})

	// GitHub Issue creation API
	mux.HandleFunc("/api/github/create-issue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
			return
		}

		// Require admin password
		if cfg.AdminPassword == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "admin password not configured"})
			return
		}

		var body struct {
			Password    string   `json:"password"`
			Title       string   `json:"title"`
			Body        string   `json:"body"`
			Labels      []string `json:"labels"`
			AppName     string   `json:"app_name"`
			ExitCode    int      `json:"exit_code"`
			ErrorText   string   `json:"error_text"`
			FailureRate float64  `json:"failure_rate"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
			return
		}

		if body.Password != cfg.AdminPassword {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "incorrect password"})
			return
		}

		if cfg.GitHubToken == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "GitHub token not configured on server"})
			return
		}

		// Build issue body if not provided
		if body.Body == "" {
			body.Body = fmt.Sprintf("## Telemetry Error Report\n\n"+
				"**Application:** %s\n"+
				"**Exit Code:** %d\n"+
				"**Error Category:** %s\n"+
				"**Failure Rate:** %.1f%%\n\n"+
				"### Error Details\n```\n%s\n```\n\n"+
				"---\n*This issue was automatically created from the telemetry error analysis dashboard.*",
				body.AppName, body.ExitCode,
				categorizeExitCode(body.ExitCode),
				body.FailureRate, body.ErrorText)
		}

		if body.Title == "" {
			body.Title = fmt.Sprintf("[Telemetry] %s: %s (exit code %d)",
				body.AppName, categorizeExitCode(body.ExitCode), body.ExitCode)
		}

		// Default labels
		if len(body.Labels) == 0 {
			body.Labels = []string{"bug", "telemetry"}
		}

		// Create GitHub issue via API
		issueURL, err := createGitHubIssue(cfg.GitHubToken, cfg.GitHubOwner, cfg.GitHubRepo, body.Title, body.Body, body.Labels)
		if err != nil {
			log.Printf("ERROR: failed to create GitHub issue: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "failed to create issue: " + err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"issue_url": issueURL,
		})
	})

	mux.HandleFunc("/telemetry", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// rate key: IP or header (header allows non-identifying keys, but header can be abused too)
		var key string
		switch cfg.RateKeyMode {
		case "header":
			key = strings.TrimSpace(r.Header.Get(cfg.RateKeyHeader))
			if key == "" {
				key = "missing"
			}
		default:
			ip := getClientIP(r, pt)
			if ip == nil {
				key = "unknown"
			} else {
				// GDPR: do NOT store IP anywhere permanent; use it only in-memory for RL key
				key = ip.String()
			}
		}
		if !rl.Allow(key) {
			log.Printf("[RATE] rejected key=%s", key)
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("[REJECT] body read error: %v", err)
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		// strict JSON decode (no unknown fields)
		var in TelemetryIn
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			// Log first 200 chars of body to help diagnose (no PII in telemetry payloads)
			snippet := string(raw)
			if len(snippet) > 200 {
				snippet = snippet[:200] + "..."
			}
			log.Printf("[REJECT] json decode: %v | body=%s", err, snippet)
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := validate(&in); err != nil {
			log.Printf("[REJECT] validation: %v | nsapp=%s status=%s", err, in.NSAPP, in.Status)
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}

		// Auto-reclassify: exit_code=0 is NEVER an error — always reclassify as success
		if in.Status == "failed" && in.ExitCode == 0 {
			in.Status = "success"
			in.Error = ""
			in.ErrorCategory = ""
			if cfg.EnableReqLogging {
				log.Printf("auto-reclassified exit_code=0 as success: nsapp=%s", in.NSAPP)
			}
		}

		// Auto-reclassify: clients still send status="failed" for SIGINT/Ctrl+C and SIGHUP,
		// detect and reclassify as "aborted" server-side.
		errorLower := strings.ToLower(in.Error)
		if in.Status == "failed" && (in.ExitCode == 129 || in.ExitCode == 130 ||
			strings.Contains(errorLower, "sigint") ||
			strings.Contains(errorLower, "ctrl+c") ||
			strings.Contains(errorLower, "ctrl-c") ||
			strings.Contains(errorLower, "sighup") ||
			strings.Contains(errorLower, "aborted by user") ||
			strings.Contains(errorLower, "user abort") ||
			strings.Contains(errorLower, "cancelled by user") ||
			strings.Contains(errorLower, "no changes have been made")) {
			in.Status = "aborted"
			if in.ErrorCategory == "" || in.ErrorCategory == "unknown" {
				in.ErrorCategory = "user_aborted"
			}
			if cfg.EnableReqLogging {
				log.Printf("auto-reclassified as aborted: nsapp=%s exit_code=%d", in.NSAPP, in.ExitCode)
			}
		}

		// Auto-categorize errors based on exit code when no category provided
		if in.Status == "failed" && (in.ErrorCategory == "" || in.ErrorCategory == "unknown") {
			if cat := getExitCodeCategory(in.ExitCode); cat != "unknown" {
				in.ErrorCategory = cat
			}
		}

		// Auto-categorize based on error text patterns when still uncategorized
		if in.Status == "failed" && (in.ErrorCategory == "" || in.ErrorCategory == "unknown") && in.Error != "" {
			in.ErrorCategory = categorizeErrorText(errorLower)
		}

		// Enrich error text with exit code description if error text is empty
		if in.Status == "failed" && in.Error == "" && in.ExitCode != 0 {
			in.Error = fmt.Sprintf("Exit code %d: %s", in.ExitCode, getExitCodeDescription(in.ExitCode))
		}

		// Track pipeline step (in-memory, written to PocketBase with each upsert)
		pKey := pipelineTrackingKey(in.ExecutionID, in.RandomID)
		pipeline := trackPipelineStep(pKey, in.Status)
		isTerminal := in.Status == "success" || in.Status == "failed" || in.Status == "aborted"
		if isTerminal {
			defer cleanupPipeline(pKey)
		}

		if cfg.EnableReqLogging {
			log.Printf("pipeline tracked: nsapp=%s status=%s key=%s steps=%d bytes=%d",
				in.NSAPP, in.Status, pKey, bytes.Count(pipeline, []byte(`"s":`)), len(pipeline))
		}

		// Map input to PocketBase schema
		out := TelemetryOut{
			RandomID:        in.RandomID,
			ExecutionID:     in.ExecutionID,
			Type:            in.Type,
			NSAPP:           in.NSAPP,
			Status:          in.Status,
			CTType:          in.CTType,
			DiskSize:        in.DiskSize,
			CoreCount:       in.CoreCount,
			RAMSize:         in.RAMSize,
			OsType:          in.OsType,
			OsVersion:       in.OsVersion,
			PveVer:          in.PveVer,
			Method:          in.Method,
			Error:           in.Error,
			ExitCode:        in.ExitCode,
			GPUVendor:       in.GPUVendor,
			GPUModel:        in.GPUModel,
			GPUPassthrough:  in.GPUPassthrough,
			CPUVendor:       in.CPUVendor,
			CPUModel:        in.CPUModel,
			RAMSpeed:        in.RAMSpeed,
			InstallDuration: in.InstallDuration,
			ErrorCategory:   in.ErrorCategory,
			RepoSource:      in.RepoSource,
			Pipeline:        pipeline,
		}
		_ = computeHash(out) // For future deduplication

		ctx, cancel := context.WithTimeout(r.Context(), cfg.RequestTimeout)
		defer cancel()

		// Upsert: Creates new record if random_id doesn't exist, updates if it does
		// repo_source is stored as a field on the record for filtering
		if err := pb.UpsertTelemetry(ctx, out); err != nil {
			// Log enough context to debug without exposing personal data (GDPR-safe)
			log.Printf("pocketbase write failed: %v | nsapp=%s type=%s status=%s ct_type=%d repo=%s method=%s",
				err, out.NSAPP, out.Type, out.Status, out.CTType, out.RepoSource, out.Method)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}

		if cfg.EnableReqLogging {
			log.Printf("telemetry accepted nsapp=%s status=%s repo=%s", out.NSAPP, out.Status, in.RepoSource)
		}

		// Don't invalidate cache on every write - the background warmup
		// refreshes every 30 minutes, which is sufficient for a dashboard.
		// Invalidating on every write caused cascading timeouts with large datasets.

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 3 * time.Second,
	}

	// Background cache warmup job
	// - On startup: full warmup (dashboard + scripts + errors, all day ranges)
	// - Every 15 min: refresh "today" data only (fast, changes frequently)
	// - Nightly at 02:00 UTC: full warmup with 23h TTL (data barely changes intra-day)
	if cfg.CacheEnabled {
		go func() {
			// Load all script stats stores from PocketBase on startup
			for _, store := range scriptStores {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				if err := store.LoadFromPB(ctx); err != nil {
					log.Printf("[STATS:%s] LoadFromPB failed: %v", store.label, err)
				}
				cancel()
			}

			// If alltime store is empty, bootstrap from raw telemetry (first run)
			if statsAllTime.IsEmpty() {
				log.Println("[STATS:alltime] Store is empty, bootstrapping...")
				ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
				if err := statsAllTime.Bootstrap(ctx, "ProxmoxVE"); err != nil {
					log.Printf("[STATS:alltime] Bootstrap failed: %v", err)
					log.Println("[STATS:alltime] Continuing with empty store - will build up incrementally")
					// Initialize with today's date to start incremental updates from now on
					today := time.Now().Format("2006-01-02")
					statsAllTime.mu.Lock()
					statsAllTime.stats = map[string]*CachedScriptStat{
						"_marker": {LastDate: today}, // Marker to prevent re-bootstrap
					}
					statsAllTime.mu.Unlock()
				}
				cancel()
			}

			// If 7d/30d stores are empty, rebuild them
			for _, store := range []*ScriptStatsStore{stats7d, stats30d} {
				if store.IsEmpty() {
					log.Printf("[STATS:%s] Store is empty, rebuilding...", store.label)
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
					if err := store.Rebuild(ctx, "ProxmoxVE"); err != nil {
						log.Printf("[STATS:%s] Rebuild failed: %v", store.label, err)
					}
					cancel()
				}
			}

			// Initial full warmup after startup
			time.Sleep(5 * time.Second)
			warmupCaches(pb, cache, cfg, false, scriptStores)

			// Periodic "today" refresh every 15 min
			todayTicker := time.NewTicker(15 * time.Minute)
			// Nightly full refresh at 02:00 UTC
			nightlyTimer := time.NewTimer(timeUntilNextUTC(2, 0))

			for {
				select {
				case <-todayTicker.C:
					warmupCaches(pb, cache, cfg, true, scriptStores)
				case <-nightlyTimer.C:
					log.Println("[CACHE] Nightly full warmup triggered")
					warmupCaches(pb, cache, cfg, false, scriptStores)
					nightlyTimer.Reset(24 * time.Hour)
				}
			}
		}()
		log.Printf("background cache warmup enabled (nightly 02:00 UTC, today refresh every 15m)")
	}

	log.Printf("telemetry-ingest listening on %s", cfg.ListenAddr)
	log.Fatal(srv.ListenAndServe())
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Minimal security headers (no cookies anyway)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// categorizeExitCode returns a short human-readable description for an exit code.
// Delegates to getExitCodeDescription (single source of truth).
func categorizeExitCode(code int) string {
	return getExitCodeDescription(code)
}

// createGitHubIssue creates a new issue in the specified GitHub repository
func createGitHubIssue(token, owner, repo, title, body string, labels []string) (string, error) {
	payload := map[string]interface{}{
		"title":  title,
		"body":   body,
		"labels": labels,
	}
	b, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("https://api.github.com/repos/%s/%s/issues", owner, repo),
		bytes.NewReader(b),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return "", fmt.Errorf("GitHub API error %d: %s", resp.StatusCode, string(rb))
	}

	var result struct {
		HTMLURL string `json:"html_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.HTMLURL, nil
}

func env(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env %s", k)
	}
	return v
}
func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	var i int
	_, _ = fmt.Sscanf(v, "%d", &i)
	if i == 0 && v != "0" {
		return def
	}
	return i
}
func envInt64(k string, def int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	var i int64
	_, _ = fmt.Sscanf(v, "%d", &i)
	if i == 0 && v != "0" {
		return def
	}
	return i
}
func envBool(k string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	var f float64
	_, _ = fmt.Sscanf(v, "%f", &f)
	if f == 0 && v != "0" {
		return def
	}
	return f
}
func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// timeUntilNextUTC calculates the duration until the next occurrence of hour:minute UTC
func timeUntilNextUTC(hour, minute int) time.Duration {
	now := time.Now().UTC()
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if now.After(next) {
		next = next.Add(24 * time.Hour)
	}
	return time.Until(next)
}

// warmupCaches pre-populates the cache for dashboard, scripts, AND errors endpoints.
// If todayOnly=true, only warms days=1 (fast refresh for current-day data).
// If todayOnly=false, warms all day ranges with long TTLs (nightly/startup).
func warmupCaches(pb *PBClient, cache *Cache, cfg Config, todayOnly bool, stores []*ScriptStatsStore) {
	label := "full"
	if todayOnly {
		label = "today-only"
	}
	log.Printf("[CACHE] Starting %s cache warmup...", label)
	start := time.Now()

	dayRanges := []int{1}
	if !todayOnly {
		dayRanges = []int{1, 90, 365} // Only Today + ranges without persistent store
	}
	repos := []string{"ProxmoxVE"}

	warmed := 0
	failed := 0

	// Update all persistent stores + cache their data on full warmup
	if !todayOnly {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		knownScripts, _ := pb.FetchKnownScripts(ctx)
		cancel()

		for _, store := range stores {
			uCtx, uCancel := context.WithTimeout(context.Background(), 600*time.Second)
			err := store.Update(uCtx, "ProxmoxVE")
			uCancel()
			if err != nil {
				log.Printf("[CACHE] store %s update failed: %v", store.label, err)
				failed++
				continue
			}

			// Build + cache the data
			data := store.BuildData(knownScripts)
			cacheKey := fmt.Sprintf("scripts:%d:ProxmoxVE", store.windowDays)
			_ = cache.Set(context.Background(), cacheKey, data, 23*time.Hour)
			warmed++
			log.Printf("[CACHE] scripts:%d cache warmed from persistent store (%s)", store.windowDays, store.label)
		}
	}

	for _, days := range dayRanges {
		cacheTTL := 2 * time.Minute
		if days > 1 {
			cacheTTL = 23 * time.Hour
		}

		// Scale timeout by data volume
		timeout := 180 * time.Second
		if days >= 365 {
			timeout = 600 * time.Second
		} else if days >= 90 {
			timeout = 300 * time.Second
		}

		for _, repo := range repos {
			// --- Dashboard ---
			{
				cacheKey := fmt.Sprintf("dashboard:%d:%s", days, repo)
				if cache.TryStartRefresh(cacheKey) {
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					data, err := pb.FetchDashboardData(ctx, days, repo)
					cancel()
					cache.FinishRefresh(cacheKey)
					if err != nil {
						log.Printf("[CACHE] Warmup dashboard failed days=%d repo=%q: %v", days, repo, err)
						failed++
					} else {
						_ = cache.Set(context.Background(), cacheKey, data, cacheTTL)
						warmed++
					}
				}
			}

			// --- Scripts ---
			{
				cacheKey := fmt.Sprintf("scripts:%d:%s", days, repo)
				if cache.TryStartRefresh(cacheKey) {
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					data, err := pb.FetchScriptAnalysisData(ctx, days, repo)
					cancel()
					cache.FinishRefresh(cacheKey)
					if err != nil {
						log.Printf("[CACHE] Warmup scripts failed days=%d repo=%q: %v", days, repo, err)
						failed++
					} else {
						_ = cache.Set(context.Background(), cacheKey, data, cacheTTL)
						warmed++
					}
				}
			}

			// --- Errors ---
			{
				cacheKey := fmt.Sprintf("errors:%d:%s", days, repo)
				if cache.TryStartRefresh(cacheKey) {
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					data, err := pb.FetchErrorAnalysisData(ctx, days, repo)
					cancel()
					cache.FinishRefresh(cacheKey)
					if err != nil {
						log.Printf("[CACHE] Warmup errors failed days=%d repo=%q: %v", days, repo, err)
						failed++
					} else {
						_ = cache.Set(context.Background(), cacheKey, data, cacheTTL)
						warmed++
					}
				}
			}
		}
	}

	log.Printf("[CACHE] Warmup %s complete: %d warmed, %d failed (took %v)", label, warmed, failed, time.Since(start).Round(time.Second))
}
