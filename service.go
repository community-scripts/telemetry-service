package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

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
	RedisURL       string
	EnableRedis    bool
	CacheTTL       time.Duration
	CacheEnabled   bool

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
}

// TelemetryIn matches payload from api.func (bash client)
type TelemetryIn struct {
	// Required
	RandomID string `json:"random_id"`         // Session UUID
	Type     string `json:"type"`              // "lxc", "vm", "tool", "addon"
	NSAPP    string `json:"nsapp"`             // Application name (e.g., "jellyfin")
	Status   string `json:"status"`            // "installing", "success", "failed", "unknown"

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
	GPUVendor       string `json:"gpu_vendor,omitempty"`       // "intel", "amd", "nvidia"
	GPUModel        string `json:"gpu_model,omitempty"`        // e.g., "Intel Arc Graphics"
	GPUPassthrough  string `json:"gpu_passthrough,omitempty"`  // "igpu", "dgpu", "vgpu", "none"

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
	RandomID  string `json:"random_id"`
	Type      string `json:"type"`
	NSAPP     string `json:"nsapp"`
	Status    string `json:"status"`
	CTType    int    `json:"ct_type,omitempty"`
	DiskSize  int    `json:"disk_size,omitempty"`
	CoreCount int    `json:"core_count,omitempty"`
	RAMSize   int    `json:"ram_size,omitempty"`
	OsType    string `json:"os_type,omitempty"`
	OsVersion string `json:"os_version,omitempty"`
	PveVer    string `json:"pve_version,omitempty"`
	Method    string `json:"method,omitempty"`
	Error     string `json:"error,omitempty"`
	ExitCode  int    `json:"exit_code,omitempty"`

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
}

// TelemetryStatusUpdate contains only fields needed for status updates
type TelemetryStatusUpdate struct {
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	ExitCode        int    `json:"exit_code"`
	InstallDuration int    `json:"install_duration,omitempty"`
	ErrorCategory   string `json:"error_category,omitempty"`
	GPUVendor       string `json:"gpu_vendor,omitempty"`
	GPUModel        string `json:"gpu_model,omitempty"`
	GPUPassthrough  string `json:"gpu_passthrough,omitempty"`
	CPUVendor       string `json:"cpu_vendor,omitempty"`
	CPUModel        string `json:"cpu_model,omitempty"`
	RAMSpeed        string `json:"ram_speed,omitempty"`
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

	// URL encode the filter
	filter := fmt.Sprintf("random_id='%s'", randomID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/collections/%s/records?filter=%s&fields=id&perPage=1",
			p.baseURL, p.targetColl, filter),
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

// FetchRecordsPaginated retrieves records with pagination and optional filters.
func (p *PBClient) FetchRecordsPaginated(ctx context.Context, page, limit int, status, app, osType, typeFilter, sortField, repoSource string) ([]TelemetryRecord, int, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, 0, err
	}

	// Build filter
	var filters []string
	if status != "" {
		filters = append(filters, fmt.Sprintf("status='%s'", status))
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
		filterStr = "&filter=" + strings.Join(filters, "&&")
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
// For status!="installing": updates existing record (found by random_id).
func (p *PBClient) UpsertTelemetry(ctx context.Context, payload TelemetryOut) error {
	// For "installing" status, always create new record
	if payload.Status == "installing" {
		return p.CreateTelemetry(ctx, payload)
	}

	// For status updates (success/failed/unknown), find and update existing record
	recordID, err := p.FindRecordByRandomID(ctx, payload.RandomID)
	if err != nil {
		// Search failed, log and return error
		return fmt.Errorf("cannot find record to update: %w", err)
	}

	if recordID == "" {
		// Record not found - this shouldn't happen normally
		// Create a full record as fallback
		return p.CreateTelemetry(ctx, payload)
	}

	// Update only status, error, exit_code, and new metrics fields
	update := TelemetryStatusUpdate{
		Status:          payload.Status,
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

func getClientIP(r *http.Request, pt *ProxyTrust) net.IP {
	// If behind reverse proxy, trust X-Forwarded-For only if remote is trusted proxy.
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	remote := net.ParseIP(host)
	if remote == nil {
		return nil
	}

	if pt != nil && pt.isTrusted(remote) {
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
	allowedType = map[string]bool{"lxc": true, "vm": true, "tool": true, "addon": true}

	// Allowed values for 'status' field
	allowedStatus = map[string]bool{"installing": true, "success": true, "failed": true, "unknown": true}

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

	// Allowed values for 'error_category' field
	allowedErrorCategory = map[string]bool{
		"network": true, "storage": true, "dependency": true, "permission": true,
		"timeout": true, "config": true, "resource": true, "unknown": true, "": true,
	}
)

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

func validate(in *TelemetryIn) error {
	// Sanitize all string fields
	in.RandomID = sanitizeShort(in.RandomID, 64)
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

	// IMPORTANT: "error" must be short and not contain identifiers/logs
	in.Error = sanitizeShort(in.Error, 120)

	// Required fields for all requests
	if in.RandomID == "" || in.Type == "" || in.NSAPP == "" || in.Status == "" {
		return errors.New("missing required fields: random_id, type, nsapp, status")
	}

	// Normalize common typos for backwards compatibility
	if in.Status == "sucess" {
		in.Status = "success"
	}

	// Validate enums
	if !allowedType[in.Type] {
		return errors.New("invalid type (must be 'lxc', 'vm', 'tool', or 'addon')")
	}
	if !allowedStatus[in.Status] {
		return errors.New("invalid status")
	}

	// Validate new enum fields
	if !allowedGPUVendor[in.GPUVendor] {
		return errors.New("invalid gpu_vendor (must be 'intel', 'amd', 'nvidia', 'unknown')")
	}
	if !allowedGPUPassthrough[in.GPUPassthrough] {
		return errors.New("invalid gpu_passthrough (must be 'igpu', 'dgpu', 'vgpu', 'none', 'unknown')")
	}
	if !allowedCPUVendor[in.CPUVendor] {
		return errors.New("invalid cpu_vendor (must be 'intel', 'amd', 'arm', 'apple', 'qualcomm', 'unknown')")
	}
	if !allowedErrorCategory[in.ErrorCategory] {
		return errors.New("invalid error_category")
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

// -------- HTTP server --------

func main() {
	cfg := Config{
		ListenAddr:         env("LISTEN_ADDR", ":8080"),
		TrustedProxiesCIDR: splitCSV(env("TRUSTED_PROXIES_CIDR", "")),

		PBBaseURL:        mustEnv("PB_URL"),
		PBAuthCollection: mustEnv("PB_AUTH_COLLECTION"),
		PBIdentity:       mustEnv("PB_IDENTITY"),
		PBPassword:       mustEnv("PB_PASSWORD"),
		PBTargetColl:     mustEnv("PB_TARGET_COLLECTION"),

		MaxBodyBytes:     envInt64("MAX_BODY_BYTES", 1024),
		RateLimitRPM:     envInt("RATE_LIMIT_RPM", 60),
		RateBurst:        envInt("RATE_BURST", 20),
		RateKeyMode:      env("RATE_KEY_MODE", "ip"), // "ip" or "header"
		RateKeyHeader:    env("RATE_KEY_HEADER", "X-Telemetry-Key"),
		RequestTimeout:   time.Duration(envInt("UPSTREAM_TIMEOUT_MS", 4000)) * time.Millisecond,
		EnableReqLogging: envBool("ENABLE_REQUEST_LOGGING", false),

		// Cache config
		RedisURL:     env("REDIS_URL", ""),
		EnableRedis:  envBool("ENABLE_REDIS", false),
		CacheTTL:     time.Duration(envInt("CACHE_TTL_SECONDS", 300)) * time.Second,
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
	}

	var pt *ProxyTrust
	if strings.TrimSpace(env("TRUSTED_PROXIES_CIDR", "")) != "" {
		p, err := NewProxyTrust(cfg.TrustedProxiesCIDR)
		if err != nil {
			log.Fatalf("invalid TRUSTED_PROXIES_CIDR: %v", err)
		}
		pt = p
	}

	pb := NewPBClient(cfg)
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
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		_, _ = w.Write([]byte(DashboardHTML()))
	})

	// Redirect /dashboard to / for backwards compatibility
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusMovedPermanently)
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
		fmt.Fprintf(w, "# HELP telemetry_installs_pending Current installing count\n")
		fmt.Fprintf(w, "# TYPE telemetry_installs_pending gauge\n")
		fmt.Fprintf(w, "telemetry_installs_pending %d\n\n", data.InstallingCount)
		fmt.Fprintf(w, "# HELP telemetry_success_rate Success rate percentage\n")
		fmt.Fprintf(w, "# TYPE telemetry_success_rate gauge\n")
		fmt.Fprintf(w, "telemetry_success_rate %.2f\n", data.SuccessRate)
	})

	// Dashboard API endpoint (with caching)
	mux.HandleFunc("/api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		days := 7 // Default: 7 days
		if d := r.URL.Query().Get("days"); d != "" {
			fmt.Sscanf(d, "%d", &days)
			// days=0 means "all entries", negative values are invalid
			if days < 0 {
				days = 7
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

		// Try cache first
		cacheKey := fmt.Sprintf("dashboard:%d:%s", days, repoSource)
		var data *DashboardData
		if cfg.CacheEnabled && cache.Get(ctx, cacheKey, &data) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			json.NewEncoder(w).Encode(data)
			return
		}

		data, err := pb.FetchDashboardData(ctx, days, repoSource)
		if err != nil {
			log.Printf("dashboard fetch failed: %v", err)
			http.Error(w, "failed to fetch data", http.StatusInternalServerError)
			return
		}

		// Cache the result
		if cfg.CacheEnabled {
			_ = cache.Set(ctx, cacheKey, data, cfg.CacheTTL)
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

		records, total, err := pb.FetchRecordsPaginated(ctx, page, limit, status, app, osType, typeFilter, sort, repoSource)
		if err != nil {
			log.Printf("records fetch failed: %v", err)
			http.Error(w, "failed to fetch records", http.StatusInternalServerError)
			return
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
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		// strict JSON decode (no unknown fields)
		var in TelemetryIn
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := validate(&in); err != nil {
			if cfg.EnableReqLogging {
				log.Printf("telemetry rejected: %v", err)
			}
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}

		// Map input to PocketBase schema
		out := TelemetryOut{
			RandomID:        in.RandomID,
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
		}
		_ = computeHash(out) // For future deduplication

		ctx, cancel := context.WithTimeout(r.Context(), cfg.RequestTimeout)
		defer cancel()

		// Upsert: Creates new record if random_id doesn't exist, updates if it does
		// repo_source is stored as a field on the record for filtering
		if err := pb.UpsertTelemetry(ctx, out); err != nil {
			// GDPR: don't log raw payload, don't log IPs; log only generic error
			log.Printf("pocketbase write failed: %v", err)
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}

		if cfg.EnableReqLogging {
			log.Printf("telemetry accepted nsapp=%s status=%s repo=%s", out.NSAPP, out.Status, in.RepoSource)
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 3 * time.Second,
	}

	// Background cache warmup job - pre-populates cache for common dashboard queries
	if cfg.CacheEnabled {
		go func() {
			// Initial warmup after startup
			time.Sleep(10 * time.Second)
			warmupDashboardCache(pb, cache, cfg)
			
			// Periodic refresh (every 4 minutes, before 5-minute TTL expires)
			ticker := time.NewTicker(4 * time.Minute)
			for range ticker.C {
				warmupDashboardCache(pb, cache, cfg)
			}
		}()
		log.Println("background cache warmup enabled")
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

// warmupDashboardCache pre-populates the cache with common dashboard queries
func warmupDashboardCache(pb *PBClient, cache *Cache, cfg Config) {
	log.Println("[CACHE] Starting dashboard cache warmup...")
	
	// Common day ranges and repos to pre-cache
	dayRanges := []int{7, 30, 90}
	repos := []string{"ProxmoxVE", ""}  // ProxmoxVE and "all"
	
	warmed := 0
	for _, days := range dayRanges {
		for _, repo := range repos {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			
			cacheKey := fmt.Sprintf("dashboard:%d:%s", days, repo)
			
			// Check if already cached
			var existing *DashboardData
			if cache.Get(ctx, cacheKey, &existing) {
				cancel()
				continue // Already cached, skip
			}
			
			// Fetch and cache
			data, err := pb.FetchDashboardData(ctx, days, repo)
			cancel()
			
			if err != nil {
				log.Printf("[CACHE] Warmup failed for days=%d repo=%s: %v", days, repo, err)
				continue
			}
			
			_ = cache.Set(context.Background(), cacheKey, data, cfg.CacheTTL)
			warmed++
			log.Printf("[CACHE] Warmed cache for days=%d repo=%s (%d installs)", days, repo, data.TotalAllTime)
		}
	}
	
	log.Printf("[CACHE] Dashboard cache warmup complete (%d entries)", warmed)
}