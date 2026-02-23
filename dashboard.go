package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// DashboardData holds aggregated statistics for the dashboard
type DashboardData struct {
	TotalInstalls   int               `json:"total_installs"`
	TotalAllTime    int               `json:"total_all_time"` // Total records in DB (not limited)
	SampleSize      int               `json:"sample_size"`    // How many records were sampled
	SuccessCount    int               `json:"success_count"`
	FailedCount     int               `json:"failed_count"`
	AbortedCount    int               `json:"aborted_count"`
	InstallingCount int               `json:"installing_count"`
	SuccessRate     float64           `json:"success_rate"`
	TopApps         []AppCount        `json:"top_apps"`
	OsDistribution  []OsCount         `json:"os_distribution"`
	MethodStats     []MethodCount     `json:"method_stats"`
	PveVersions     []PveCount        `json:"pve_versions"`
	TypeStats       []TypeCount       `json:"type_stats"`
	ErrorAnalysis   []ErrorGroup      `json:"error_analysis"`
	FailedApps      []AppFailure      `json:"failed_apps"`
	RecentRecords   []TelemetryRecord `json:"recent_records"`
	DailyStats      []DailyStat       `json:"daily_stats"`

	// Extended metrics
	GPUStats           []GPUCount      `json:"gpu_stats"`
	ErrorCategories    []ErrorCatCount `json:"error_categories"`
	TopTools           []ToolCount     `json:"top_tools"`
	TopAddons          []AddonCount    `json:"top_addons"`
	AvgInstallDuration float64         `json:"avg_install_duration"` // seconds
	TotalTools         int             `json:"total_tools"`
	TotalAddons        int             `json:"total_addons"`
}

type AppCount struct {
	App   string `json:"app"`
	Count int    `json:"count"`
}

type OsCount struct {
	Os    string `json:"os"`
	Count int    `json:"count"`
}

type MethodCount struct {
	Method string `json:"method"`
	Count  int    `json:"count"`
}

type PveCount struct {
	Version string `json:"version"`
	Count   int    `json:"count"`
}

type TypeCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

type ErrorGroup struct {
	Pattern    string `json:"pattern"`
	Count      int    `json:"count"`       // Total error occurrences
	UniqueApps int    `json:"unique_apps"` // Number of unique apps affected
	Apps       string `json:"apps"`        // Comma-separated list of affected apps
}

type AppFailure struct {
	App         string  `json:"app"`
	Type        string  `json:"type"`
	TotalCount  int     `json:"total_count"`
	FailedCount int     `json:"failed_count"`
	FailureRate float64 `json:"failure_rate"`
}

type DailyStat struct {
	Date    string `json:"date"`
	Success int    `json:"success"`
	Failed  int    `json:"failed"`
}

// Extended metric types
type GPUCount struct {
	Vendor      string `json:"vendor"`
	Passthrough string `json:"passthrough"`
	Count       int    `json:"count"`
}

type ErrorCatCount struct {
	Category string `json:"category"`
	Count    int    `json:"count"`
}

type ToolCount struct {
	Tool  string `json:"tool"`
	Count int    `json:"count"`
}

type AddonCount struct {
	Addon string `json:"addon"`
	Count int    `json:"count"`
}

// ========================================================
// Error Analysis Data Types
// ========================================================

// ErrorAnalysisData holds comprehensive error analysis
type ErrorAnalysisData struct {
	TotalErrors     int                  `json:"total_errors"`
	TotalInstalls   int                  `json:"total_installs"`
	OverallFailRate float64              `json:"overall_fail_rate"`
	ExitCodeStats   []ExitCodeStat       `json:"exit_code_stats"`
	CategoryStats   []CategoryStat       `json:"category_stats"`
	AppErrors       []AppErrorDetail     `json:"app_errors"`
	RecentErrors    []ErrorRecord        `json:"recent_errors"`
	StuckInstalling int                  `json:"stuck_installing"`
	ErrorTimeline   []ErrorTimelinePoint `json:"error_timeline"`
}

type ExitCodeStat struct {
	ExitCode    int     `json:"exit_code"`
	Count       int     `json:"count"`
	Description string  `json:"description"`
	Category    string  `json:"category"`
	Percentage  float64 `json:"percentage"`
}

type CategoryStat struct {
	Category   string  `json:"category"`
	Count      int     `json:"count"`
	Percentage float64 `json:"percentage"`
	TopApps    string  `json:"top_apps"`
}

type AppErrorDetail struct {
	App          string  `json:"app"`
	Type         string  `json:"type"`
	TotalCount   int     `json:"total_count"`
	FailedCount  int     `json:"failed_count"`
	AbortedCount int     `json:"aborted_count"`
	FailureRate  float64 `json:"failure_rate"`
	TopExitCode  int     `json:"top_exit_code"`
	TopError     string  `json:"top_error"`
	TopCategory  string  `json:"top_category"`
}

type ErrorRecord struct {
	NSAPP         string `json:"nsapp"`
	Type          string `json:"type"`
	Status        string `json:"status"`
	ExitCode      int    `json:"exit_code"`
	Error         string `json:"error"`
	ErrorCategory string `json:"error_category"`
	OsType        string `json:"os_type"`
	OsVersion     string `json:"os_version"`
	Created       string `json:"created"`
}

type ErrorTimelinePoint struct {
	Date    string `json:"date"`
	Failed  int    `json:"failed"`
	Aborted int    `json:"aborted"`
}

// ========================================================
// Script Analysis Data Types
// ========================================================

// ScriptInfo holds slug, type, and creation date for known scripts
type ScriptInfo struct {
	Slug    string
	Type    string // "ct", "vm", "pve", "addon", "turnkey"
	Created time.Time
}

// type relation ID -> display type mapping
var scriptTypeIDMap = map[string]string{
	"nm9bra8mzye2scg": "ct",
	"lte524abgx960bd": "vm",
	"1uyjfno0fpf5buh": "pve",
	"88xtxy57q80v38v": "addon",
	"fbwvn9nhe3lmc9l": "turnkey",
}

// FetchKnownScripts fetches all slugs and types from script_scripts collection
func (p *PBClient) FetchKnownScripts(ctx context.Context) (map[string]ScriptInfo, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, err
	}

	scripts := make(map[string]ScriptInfo)
	page := 1
	perPage := 500

	for {
		reqURL := fmt.Sprintf("%s/api/collections/script_scripts/records?fields=slug,type,script_created&page=%d&perPage=%d",
			p.baseURL, page, perPage)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+p.token)

		resp, err := p.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HTTP request to script_scripts failed: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body := make([]byte, 512)
			n, _ := resp.Body.Read(body)
			resp.Body.Close()
			return nil, fmt.Errorf("script_scripts returned HTTP %d: %s", resp.StatusCode, string(body[:n]))
		}

		var result struct {
			Items []struct {
				Slug          string `json:"slug"`
				Type          string `json:"type"`
				ScriptCreated string `json:"script_created"`
			} `json:"items"`
			TotalItems int `json:"totalItems"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		for _, item := range result.Items {
			if item.Slug != "" {
				displayType := scriptTypeIDMap[item.Type]
				if displayType == "" {
					displayType = item.Type
				}
				created := time.Time{}
				if item.ScriptCreated != "" {
					if t, err := time.Parse("2006-01-02 15:04:05.000Z", item.ScriptCreated); err == nil {
						created = t
					} else if t, err := time.Parse("2006-01-02", item.ScriptCreated[:10]); err == nil {
						created = t
					}
				}
				scripts[item.Slug] = ScriptInfo{Slug: item.Slug, Type: displayType, Created: created}
			}
		}

		if len(scripts) >= result.TotalItems || len(result.Items) == 0 {
			break
		}
		page++
	}

	return scripts, nil
}

type ScriptAnalysisData struct {
	TotalScripts  int            `json:"total_scripts"`
	TotalInstalls int            `json:"total_installs"`
	TopScripts    []ScriptStat   `json:"top_scripts"`
	RecentScripts []RecentScript `json:"recent_scripts"`
}

type ScriptStat struct {
	App            string  `json:"app"`
	Type           string  `json:"type"`
	Total          int     `json:"total"`
	Success        int     `json:"success"`
	Failed         int     `json:"failed"`
	Aborted        int     `json:"aborted"`
	Installing     int     `json:"installing"`
	SuccessRate    float64 `json:"success_rate"`
	DaysOld        int     `json:"days_old"`
	InstallsPerDay float64 `json:"installs_per_day"`
}

type RecentScript struct {
	App       string `json:"app"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	ExitCode  int    `json:"exit_code"`
	OsType    string `json:"os_type"`
	OsVersion string `json:"os_version"`
	PveVer    string `json:"pve_version"`
	Created   string `json:"created"`
	Method    string `json:"method"`
}

// ========================================================
// Script Stats Store (persistent in PocketBase collections)
// Used for _script_stats_7d, _script_stats_30d, _script_stats_alltime
// ========================================================

// CachedScriptStat represents one row in a _script_stats_* collection
type CachedScriptStat struct {
	ID         string `json:"id,omitempty"`
	Slug       string `json:"slug"`
	Type       string `json:"type"`
	Total      int    `json:"total"`
	Success    int    `json:"success"`
	Failed     int    `json:"failed"`
	Aborted    int    `json:"aborted"`
	Installing int    `json:"installing"`
	LastDate   string `json:"last_date"`
}

// ScriptStatsStore manages persistent script stats for a specific time window
type ScriptStatsStore struct {
	mu         sync.RWMutex
	stats      map[string]*CachedScriptStat // key = slug
	pb         *PBClient
	collection string // PocketBase collection name (e.g. "_script_stats_7d")
	windowDays int    // 7, 30, or 0 (0 = all-time / incremental)
	label      string // log label
}

func NewScriptStatsStore(pb *PBClient, collection string, windowDays int) *ScriptStatsStore {
	label := fmt.Sprintf("%dd", windowDays)
	if windowDays == 0 {
		label = "alltime"
	}
	return &ScriptStatsStore{
		stats:      make(map[string]*CachedScriptStat),
		pb:         pb,
		collection: collection,
		windowDays: windowDays,
		label:      label,
	}
}

// LoadFromPB reads all existing rows from the collection
func (s *ScriptStatsStore) LoadFromPB(ctx context.Context) error {
	if err := s.pb.ensureAuth(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats = make(map[string]*CachedScriptStat)
	page := 1
	perPage := 500

	for {
		reqURL := fmt.Sprintf("%s/api/collections/%s/records?page=%d&perPage=%d",
			s.pb.baseURL, s.collection, page, perPage)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+s.pb.token)

		resp, err := s.pb.http.Do(req)
		if err != nil {
			return fmt.Errorf("[STATS:%s] load failed: %w", s.label, err)
		}

		if resp.StatusCode != http.StatusOK {
			body := make([]byte, 512)
			n, _ := resp.Body.Read(body)
			resp.Body.Close()
			return fmt.Errorf("[STATS:%s] %s returned HTTP %d: %s", s.label, s.collection, resp.StatusCode, string(body[:n]))
		}

		var result struct {
			Items      []CachedScriptStat `json:"items"`
			TotalItems int                `json:"totalItems"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return err
		}
		resp.Body.Close()

		for i := range result.Items {
			item := result.Items[i]
			s.stats[item.Slug] = &item
		}

		if len(s.stats) >= result.TotalItems || len(result.Items) == 0 {
			break
		}
		page++
	}

	log.Printf("[STATS:%s] Loaded %d script stats from %s", s.label, len(s.stats), s.collection)
	return nil
}

// IsEmpty returns true if no stats are loaded
func (s *ScriptStatsStore) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.stats) == 0
}

// GetLastDate returns the most recent last_date across all stats
func (s *ScriptStatsStore) GetLastDate() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	best := ""
	for _, st := range s.stats {
		if st.LastDate > best {
			best = st.LastDate
		}
	}
	return best
}

// reclassifyRecord applies status reclassification rules
func reclassifyRecord(r *TelemetryRecord) {
	if r.Status == "failed" && (r.ExitCode == 129 || r.ExitCode == 130 ||
		strings.Contains(strings.ToLower(r.Error), "sighup") ||
		strings.Contains(strings.ToLower(r.Error), "sigint") ||
		strings.Contains(strings.ToLower(r.Error), "ctrl+c") ||
		strings.Contains(strings.ToLower(r.Error), "aborted by user")) {
		r.Status = "aborted"
	}
	if r.Status == "failed" && r.ExitCode == 0 && (r.Error == "" || strings.ToLower(r.Error) == "success") {
		r.Status = "success"
	}
}

// aggregateRecords aggregates telemetry records into per-script stats
func aggregateRecords(records []TelemetryRecord, knownScripts map[string]ScriptInfo) map[string]*CachedScriptStat {
	agg := make(map[string]*CachedScriptStat)
	for i := range records {
		r := &records[i]
		if knownScripts != nil {
			if _, ok := knownScripts[r.NSAPP]; !ok {
				continue
			}
		}
		reclassifyRecord(r)
		st := agg[r.NSAPP]
		if st == nil {
			st = &CachedScriptStat{Slug: r.NSAPP, Type: r.Type}
			agg[r.NSAPP] = st
		}
		st.Total++
		switch r.Status {
		case "success":
			st.Success++
		case "failed":
			st.Failed++
		case "aborted":
			st.Aborted++
		case "installing", "validation", "configuring":
			st.Installing++
		}
	}
	return agg
}

// Rebuild fetches records for the last windowDays and replaces all stats (for 7d/30d)
func (s *ScriptStatsStore) Rebuild(ctx context.Context, repoSource string) error {
	if s.windowDays <= 0 {
		return fmt.Errorf("Rebuild only works for windowed stores (windowDays > 0)")
	}

	log.Printf("[STATS:%s] Rebuilding from last %d days of telemetry...", s.label, s.windowDays)

	if err := s.pb.ensureAuth(ctx); err != nil {
		return err
	}

	since := time.Now().AddDate(0, 0, -s.windowDays).Format("2006-01-02")
	var filterParts []string
	filterParts = append(filterParts, fmt.Sprintf("created >= '%s 00:00:00'", since))
	if repoSource != "" {
		filterParts = append(filterParts, fmt.Sprintf("repo_source = '%s'", repoSource))
	}
	filter := url.QueryEscape(strings.Join(filterParts, " && "))

	result, err := s.pb.fetchRecords(ctx, filter)
	if err != nil {
		return fmt.Errorf("[STATS:%s] rebuild fetch failed: %w", s.label, err)
	}

	knownScripts, _ := s.pb.FetchKnownScripts(ctx)
	agg := aggregateRecords(result.Records, knownScripts)
	today := time.Now().Format("2006-01-02")

	s.mu.Lock()
	// Reset existing stats to 0 but keep PB record IDs
	for _, st := range s.stats {
		st.Total, st.Success, st.Failed, st.Aborted, st.Installing = 0, 0, 0, 0, 0
	}
	// Apply new aggregation
	for slug, fresh := range agg {
		if existing, ok := s.stats[slug]; ok {
			existing.Total = fresh.Total
			existing.Success = fresh.Success
			existing.Failed = fresh.Failed
			existing.Aborted = fresh.Aborted
			existing.Installing = fresh.Installing
			existing.Type = fresh.Type
			existing.LastDate = today
		} else {
			fresh.LastDate = today
			s.stats[slug] = fresh
		}
	}
	// Update last_date for all
	for _, st := range s.stats {
		st.LastDate = today
	}
	s.mu.Unlock()

	if err := s.writeAllToPB(ctx); err != nil {
		return err
	}

	log.Printf("[STATS:%s] Rebuild complete: %d scripts from %d records", s.label, len(agg), len(result.Records))
	return nil
}

// Bootstrap does a full aggregation (all-time, no date filter) — first run only
func (s *ScriptStatsStore) Bootstrap(ctx context.Context, repoSource string) error {
	log.Printf("[STATS:%s] Bootstrap: loading ALL telemetry records (this may take a while)...", s.label)

	if err := s.pb.ensureAuth(ctx); err != nil {
		return err
	}

	var filterParts []string
	if repoSource != "" {
		filterParts = append(filterParts, fmt.Sprintf("repo_source = '%s'", repoSource))
	}
	var filter string
	if len(filterParts) > 0 {
		filter = url.QueryEscape(strings.Join(filterParts, " && "))
	}

	result, err := s.pb.fetchRecords(ctx, filter)
	if err != nil {
		return fmt.Errorf("[STATS:%s] bootstrap fetch failed: %w", s.label, err)
	}

	knownScripts, _ := s.pb.FetchKnownScripts(ctx)
	agg := aggregateRecords(result.Records, knownScripts)
	today := time.Now().Format("2006-01-02")

	s.mu.Lock()
	s.stats = make(map[string]*CachedScriptStat)
	for slug, st := range agg {
		st.LastDate = today
		s.stats[slug] = st
	}
	s.mu.Unlock()

	if err := s.writeAllToPB(ctx); err != nil {
		return err
	}

	log.Printf("[STATS:%s] Bootstrap complete: %d scripts aggregated from %d records", s.label, len(agg), len(result.Records))
	return nil
}

// IncrementalUpdate fetches records since lastDate and adds counts (all-time only)
func (s *ScriptStatsStore) IncrementalUpdate(ctx context.Context, repoSource string) error {
	lastDate := s.GetLastDate()
	if lastDate == "" {
		return s.Bootstrap(ctx, repoSource)
	}

	log.Printf("[STATS:%s] Incremental update since %s...", s.label, lastDate)

	if err := s.pb.ensureAuth(ctx); err != nil {
		return err
	}

	var filterParts []string
	filterParts = append(filterParts, fmt.Sprintf("created >= '%s 00:00:00'", lastDate))
	if repoSource != "" {
		filterParts = append(filterParts, fmt.Sprintf("repo_source = '%s'", repoSource))
	}
	filter := url.QueryEscape(strings.Join(filterParts, " && "))

	result, err := s.pb.fetchRecords(ctx, filter)
	if err != nil {
		return fmt.Errorf("[STATS:%s] incremental fetch failed: %w", s.label, err)
	}

	knownScripts, _ := s.pb.FetchKnownScripts(ctx)
	today := time.Now().Format("2006-01-02")

	s.mu.Lock()
	added := 0
	for i := range result.Records {
		r := &result.Records[i]

		if knownScripts != nil {
			if _, ok := knownScripts[r.NSAPP]; !ok {
				continue
			}
		}

		reclassifyRecord(r)

		st := s.stats[r.NSAPP]
		if st == nil {
			st = &CachedScriptStat{Slug: r.NSAPP, Type: r.Type}
			s.stats[r.NSAPP] = st
		}
		st.Total++
		switch r.Status {
		case "success":
			st.Success++
		case "failed":
			st.Failed++
		case "aborted":
			st.Aborted++
		case "installing", "validation", "configuring":
			st.Installing++
		}
		added++
	}
	for _, st := range s.stats {
		st.LastDate = today
	}
	s.mu.Unlock()

	if err := s.writeAllToPB(ctx); err != nil {
		return err
	}

	log.Printf("[STATS:%s] Incremental update complete: %d new records, %d scripts total", s.label, added, len(s.stats))
	return nil
}

// Update dispatches to Rebuild (windowed) or IncrementalUpdate (all-time)
func (s *ScriptStatsStore) Update(ctx context.Context, repoSource string) error {
	if s.windowDays > 0 {
		return s.Rebuild(ctx, repoSource)
	}
	return s.IncrementalUpdate(ctx, repoSource)
}

// writeAllToPB upserts all stats to the collection
func (s *ScriptStatsStore) writeAllToPB(ctx context.Context) error {
	if err := s.pb.ensureAuth(ctx); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	written := 0
	for _, st := range s.stats {
		body, _ := json.Marshal(st)

		var reqURL string
		var method string
		if st.ID != "" {
			reqURL = fmt.Sprintf("%s/api/collections/%s/records/%s", s.pb.baseURL, s.collection, st.ID)
			method = http.MethodPatch
		} else {
			reqURL = fmt.Sprintf("%s/api/collections/%s/records", s.pb.baseURL, s.collection)
			method = http.MethodPost
		}

		req, err := http.NewRequestWithContext(ctx, method, reqURL, strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+s.pb.token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.pb.http.Do(req)
		if err != nil {
			return fmt.Errorf("[STATS:%s] write %s failed: %w", s.label, st.Slug, err)
		}

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == 204 {
			if method == http.MethodPost {
				var created struct {
					ID string `json:"id"`
				}
				json.NewDecoder(resp.Body).Decode(&created)
				st.ID = created.ID
			}
			resp.Body.Close()
			written++
		} else {
			errBody := make([]byte, 512)
			n, _ := resp.Body.Read(errBody)
			resp.Body.Close()
			return fmt.Errorf("[STATS:%s] write %s returned HTTP %d: %s", s.label, st.Slug, resp.StatusCode, string(errBody[:n]))
		}
	}

	log.Printf("[STATS:%s] Wrote %d stats to %s", s.label, written, s.collection)
	return nil
}

// BuildData constructs ScriptAnalysisData from the persistent store
func (s *ScriptStatsStore) BuildData(knownScripts map[string]ScriptInfo) *ScriptAnalysisData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := &ScriptAnalysisData{}
	now := time.Now()
	seen := make(map[string]bool)

	for _, st := range s.stats {
		if st.Total == 0 {
			continue // skip zeroed-out stats from previous windows
		}
		seen[st.Slug] = true
		rate := float64(0)
		completed := st.Success + st.Failed + st.Aborted
		if completed > 0 {
			rate = float64(st.Success) / float64(completed) * 100
		}
		daysOld := 0
		installsPerDay := float64(0)
		if knownScripts != nil {
			if info, ok := knownScripts[st.Slug]; ok && !info.Created.IsZero() {
				daysOld = int(now.Sub(info.Created).Hours() / 24)
				if daysOld < 1 {
					daysOld = 1
				}
				installsPerDay = float64(st.Total) / float64(daysOld)
			}
		}
		typ := st.Type
		if knownScripts != nil {
			if info, ok := knownScripts[st.Slug]; ok {
				typ = info.Type
			}
		}
		data.TopScripts = append(data.TopScripts, ScriptStat{
			App:            st.Slug,
			Type:           typ,
			Total:          st.Total,
			Success:        st.Success,
			Failed:         st.Failed,
			Aborted:        st.Aborted,
			Installing:     st.Installing,
			SuccessRate:    rate,
			DaysOld:        daysOld,
			InstallsPerDay: installsPerDay,
		})
		data.TotalInstalls += st.Total
	}

	// Add zero-usage scripts (only for 30d and alltime)
	if knownScripts != nil && (s.windowDays >= 30 || s.windowDays == 0) {
		for slug, info := range knownScripts {
			if !seen[slug] {
				daysOld := 0
				if !info.Created.IsZero() {
					daysOld = int(now.Sub(info.Created).Hours() / 24)
					if daysOld < 1 {
						daysOld = 1
					}
				}
				data.TopScripts = append(data.TopScripts, ScriptStat{
					App:     slug,
					Type:    info.Type,
					DaysOld: daysOld,
				})
			}
		}
	}

	data.TotalScripts = len(data.TopScripts)

	// Sort by total desc
	for i := 0; i < len(data.TopScripts); i++ {
		for j := i + 1; j < len(data.TopScripts); j++ {
			if data.TopScripts[j].Total > data.TopScripts[i].Total {
				data.TopScripts[i], data.TopScripts[j] = data.TopScripts[j], data.TopScripts[i]
			}
		}
	}

	return data
}

// FetchScriptAnalysisData retrieves script usage statistics
func (p *PBClient) FetchScriptAnalysisData(ctx context.Context, days int, repoSource string) (*ScriptAnalysisData, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, err
	}

	// Fetch known scripts from script_scripts to filter against
	knownScripts, err := p.FetchKnownScripts(ctx)
	if err != nil {
		log.Printf("[ERROR] could not fetch known scripts: %v — script filter will not be applied", err)
		knownScripts = nil
	} else {
		log.Printf("[INFO] loaded %d known scripts for filtering", len(knownScripts))
	}

	var filterParts []string
	if days > 0 {
		var since string
		if days == 1 {
			since = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			since = time.Now().AddDate(0, 0, -(days-1)).Format("2006-01-02") + " 00:00:00"
		}
		filterParts = append(filterParts, fmt.Sprintf("created >= '%s'", since))
	}
	if repoSource != "" {
		filterParts = append(filterParts, fmt.Sprintf("repo_source = '%s'", repoSource))
	}

	var filter string
	if len(filterParts) > 0 {
		filter = url.QueryEscape(strings.Join(filterParts, " && "))
	}

	result, err := p.fetchRecords(ctx, filter)
	if err != nil {
		return nil, err
	}

	// Filter records to only known scripts (if slug list available)
	var records []TelemetryRecord
	if knownScripts != nil && len(knownScripts) > 0 {
		for _, r := range result.Records {
			if _, ok := knownScripts[r.NSAPP]; ok {
				records = append(records, r)
			}
		}
	} else {
		records = result.Records
	}

	data := &ScriptAnalysisData{
		TotalInstalls: len(records),
	}

	type accumulator struct {
		app        string
		typ        string
		total      int
		success    int
		failed     int
		aborted    int
		installing int
	}

	appStats := make(map[string]*accumulator)
	uniqueApps := make(map[string]bool)
	var recentAll []RecentScript

	for i := range records {
		r := &records[i]

		// Auto-reclassify SIGINT/SIGHUP as aborted
		if r.Status == "failed" && (r.ExitCode == 129 || r.ExitCode == 130 ||
			strings.Contains(strings.ToLower(r.Error), "sighup") ||
			strings.Contains(strings.ToLower(r.Error), "sigint") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl+c") ||
			strings.Contains(strings.ToLower(r.Error), "aborted by user")) {
			r.Status = "aborted"
		}
		// Reclassify failed+exit_code=0 — exit_code=0 is NEVER an error
		if r.Status == "failed" && r.ExitCode == 0 {
			r.Status = "success"
		}

		key := r.NSAPP + "|" + r.Type
		uniqueApps[r.NSAPP] = true
		if appStats[key] == nil {
			appStats[key] = &accumulator{app: r.NSAPP, typ: r.Type}
		}
		a := appStats[key]
		a.total++
		switch r.Status {
		case "success":
			a.success++
		case "failed":
			a.failed++
		case "aborted":
			a.aborted++
		case "installing", "validation", "configuring":
			a.installing++
		}

		// Collect recent records (max 200)
		if len(recentAll) < 200 {
			recentAll = append(recentAll, RecentScript{
				App:       r.NSAPP,
				Type:      r.Type,
				Status:    r.Status,
				ExitCode:  r.ExitCode,
				OsType:    r.OsType,
				OsVersion: r.OsVersion,
				PveVer:    r.PveVer,
				Created:   r.Created,
				Method:    r.Method,
			})
		}
	}

	data.TotalScripts = len(uniqueApps)

	// Build sorted script stats (by total desc)
	now := time.Now()
	for _, a := range appStats {
		rate := float64(0)
		completed := a.success + a.failed + a.aborted
		if completed > 0 {
			rate = float64(a.success) / float64(completed) * 100
		}
		daysOld := 0
		installsPerDay := float64(0)
		if knownScripts != nil {
			if info, ok := knownScripts[a.app]; ok && !info.Created.IsZero() {
				daysOld = int(now.Sub(info.Created).Hours() / 24)
				if daysOld < 1 {
					daysOld = 1
				}
				installsPerDay = float64(a.total) / float64(daysOld)
			}
		}
		data.TopScripts = append(data.TopScripts, ScriptStat{
			App:            a.app,
			Type:           a.typ,
			Total:          a.total,
			Success:        a.success,
			Failed:         a.failed,
			Aborted:        a.aborted,
			Installing:     a.installing,
			SuccessRate:    rate,
			DaysOld:        daysOld,
			InstallsPerDay: installsPerDay,
		})
	}

	// Add zero-usage scripts from script_scripts (only for 30d+ and All Time, not 7d)
	if knownScripts != nil && (days == 0 || days >= 30) {
		for slug, info := range knownScripts {
			if !uniqueApps[slug] {
				daysOld := 0
				if !info.Created.IsZero() {
					daysOld = int(now.Sub(info.Created).Hours() / 24)
					if daysOld < 1 {
						daysOld = 1
					}
				}
				data.TopScripts = append(data.TopScripts, ScriptStat{
					App:            slug,
					Type:           info.Type,
					Total:          0,
					Success:        0,
					Failed:         0,
					Aborted:        0,
					Installing:     0,
					SuccessRate:    0,
					DaysOld:        daysOld,
					InstallsPerDay: 0,
				})
				data.TotalScripts++
			}
		}
	}

	// Sort by total desc
	for i := 0; i < len(data.TopScripts); i++ {
		for j := i + 1; j < len(data.TopScripts); j++ {
			if data.TopScripts[j].Total > data.TopScripts[i].Total {
				data.TopScripts[i], data.TopScripts[j] = data.TopScripts[j], data.TopScripts[i]
			}
		}
	}

	data.RecentScripts = recentAll
	return data, nil
}

// FetchErrorAnalysisData retrieves detailed error analysis from PocketBase
func (p *PBClient) FetchErrorAnalysisData(ctx context.Context, days int, repoSource string) (*ErrorAnalysisData, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, err
	}

	// Build filter
	var filterParts []string
	if days > 0 {
		var since string
		if days == 1 {
			since = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			since = time.Now().AddDate(0, 0, -(days-1)).Format("2006-01-02") + " 00:00:00"
		}
		filterParts = append(filterParts, fmt.Sprintf("created >= '%s'", since))
	}
	if repoSource != "" {
		filterParts = append(filterParts, fmt.Sprintf("repo_source = '%s'", repoSource))
	}

	var filter string
	if len(filterParts) > 0 {
		filter = url.QueryEscape(strings.Join(filterParts, " && "))
	}

	// Fetch all records
	result, err := p.fetchRecords(ctx, filter)
	if err != nil {
		return nil, err
	}
	records := result.Records

	data := &ErrorAnalysisData{}
	data.TotalInstalls = len(records)

	// Analysis maps
	exitCodeCounts := make(map[int]int)
	categoryCounts := make(map[string]int)
	categoryApps := make(map[string]map[string]bool)
	appStats := make(map[string]*appStatAccum)
	dailyFailed := make(map[string]int)
	dailyAborted := make(map[string]int)
	var recentErrors []ErrorRecord
	stuckCount := 0

	for i := range records {
		r := &records[i]

		// Auto-reclassify (same logic as dashboard) — SIGHUP + SIGINT = aborted
		if r.Status == "failed" && (r.ExitCode == 129 || r.ExitCode == 130 ||
			strings.Contains(strings.ToLower(r.Error), "sighup") ||
			strings.Contains(strings.ToLower(r.Error), "sigint") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl+c") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl-c") ||
			strings.Contains(strings.ToLower(r.Error), "aborted by user") ||
			strings.Contains(strings.ToLower(r.Error), "no changes have been made")) {
			r.Status = "aborted"
		}

		// Reclassify: exit_code=0 is NEVER an error — always reclassify as success
		if r.Status == "failed" && r.ExitCode == 0 {
			r.Status = "success"
		}

		if r.Status == "installing" || r.Status == "validation" || r.Status == "configuring" {
			stuckCount++
			continue
		}

		if r.Status != "failed" && r.Status != "aborted" {
			// Track total for app stats
			key := r.NSAPP + "|" + r.Type
			if appStats[key] == nil {
				appStats[key] = &appStatAccum{app: r.NSAPP, typ: r.Type}
			}
			appStats[key].total++
			continue
		}

		// This is a failed or aborted record
		data.TotalErrors++

		// Exit code stats
		if r.Status == "failed" {
			exitCodeCounts[r.ExitCode]++
		}

		// Category stats
		cat := r.ErrorCategory
		if cat == "" {
			cat = "uncategorized"
		}
		categoryCounts[cat]++
		if categoryApps[cat] == nil {
			categoryApps[cat] = make(map[string]bool)
		}
		if r.NSAPP != "" {
			categoryApps[cat][r.NSAPP] = true
		}

		// App stats
		key := r.NSAPP + "|" + r.Type
		if appStats[key] == nil {
			appStats[key] = &appStatAccum{app: r.NSAPP, typ: r.Type}
		}
		appStats[key].total++
		if r.Status == "failed" {
			appStats[key].failed++
		} else {
			appStats[key].aborted++
		}
		// Track top error per app
		if r.ExitCode != 0 && (appStats[key].topExitCodeCount == 0 || appStats[key].topExitCodeCount < exitCodeCounts[r.ExitCode]) {
			appStats[key].topExitCode = r.ExitCode
			appStats[key].topExitCodeCount = exitCodeCounts[r.ExitCode]
		}
		if r.Error != "" && (appStats[key].topError == "" || len(r.Error) > len(appStats[key].topError)) {
			appStats[key].topError = r.Error
		}
		if cat != "uncategorized" && appStats[key].topCategory == "" {
			appStats[key].topCategory = cat
		}

		// Daily timeline
		if r.Created != "" {
			date := r.Created[:10]
			if r.Status == "failed" {
				dailyFailed[date]++
			} else {
				dailyAborted[date]++
			}
		}

		// Collect recent errors (up to 100)
		if len(recentErrors) < 100 {
			recentErrors = append(recentErrors, ErrorRecord{
				NSAPP:         r.NSAPP,
				Type:          r.Type,
				Status:        r.Status,
				ExitCode:      r.ExitCode,
				Error:         r.Error,
				ErrorCategory: r.ErrorCategory,
				OsType:        r.OsType,
				OsVersion:     r.OsVersion,
				Created:       r.Created,
			})
		}
	}

	data.StuckInstalling = stuckCount

	// Overall fail rate
	if data.TotalInstalls > 0 {
		data.OverallFailRate = float64(data.TotalErrors) / float64(data.TotalInstalls) * 100
	}

	// Build exit code stats
	for code, count := range exitCodeCounts {
		desc := "Unknown"
		cat := "unknown"
		// Use the exit code descriptions and categories from service.go
		if code == 0 {
			// exit_code=0 is Success — skip from error stats
			continue
		}
		switch code {
		case 1:
			desc = "General error"
			cat = "unknown"
		case 2:
			desc = "Misuse of shell builtins"
			cat = "unknown"
		case 4:
			desc = "curl: Network/protocol error"
			cat = "network"
		case 5:
			desc = "curl: Could not resolve proxy"
			cat = "network"
		case 6:
			desc = "curl: Could not resolve host"
			cat = "network"
		case 7:
			desc = "curl: Connection refused"
			cat = "network"
		case 8:
			desc = "curl: FTP server reply error"
			cat = "network"
		case 10:
			desc = "Docker / privileged mode required"
			cat = "config"
		case 22:
			desc = "curl: HTTP error (404/500 etc.)"
			cat = "network"
		case 23:
			desc = "curl: Write error (disk full?)"
			cat = "storage"
		case 25:
			desc = "curl: Upload failed"
			cat = "network"
		case 28:
			desc = "curl: Connection timed out"
			cat = "timeout"
		case 30:
			desc = "curl: FTP port command failed"
			cat = "network"
		case 35:
			desc = "SSL connect error"
			cat = "network"
		case 56:
			desc = "curl: Receive error (connection reset)"
			cat = "network"
		case 75:
			desc = "Temporary failure (retry later)"
			cat = "unknown"
		case 78:
			desc = "curl: Remote file not found (404)"
			cat = "network"
		case 100:
			desc = "APT: Package manager error (broken packages / dependency problems)"
			cat = "apt"
		case 101:
			desc = "APT: Unmet dependencies"
			cat = "apt"
		case 102:
			desc = "APT: Lock held by another process"
			cat = "apt"
		case 124:
			desc = "Command timed out (timeout command)"
			cat = "timeout"
		case 125:
			desc = "Docker daemon error (container failed to run)"
			cat = "config"
		case 126:
			desc = "Command cannot execute (permission problem)"
			cat = "permission"
		case 127:
			desc = "Command not found"
			cat = "command_not_found"
		case 128:
			desc = "Invalid argument to exit"
			cat = "signal"
		case 129:
			desc = "Killed by SIGHUP (terminal closed / hangup)"
			cat = "user_aborted"
		case 130:
			desc = "Script terminated by Ctrl+C (SIGINT)"
			cat = "user_aborted"
		case 131:
			desc = "Killed by SIGQUIT (core dump)"
			cat = "signal"
		case 134:
			desc = "Process aborted (SIGABRT)"
			cat = "signal"
		case 137:
			desc = "Process killed (SIGKILL) - likely OOM"
			cat = "resource"
		case 139:
			desc = "Segmentation fault (SIGSEGV)"
			cat = "unknown"
		case 141:
			desc = "Broken pipe (SIGPIPE)"
			cat = "signal"
		case 143:
			desc = "Process terminated (SIGTERM)"
			cat = "signal"
		// Systemd / Service errors
		case 150:
			desc = "Systemd: Service failed to start"
			cat = "service"
		case 151:
			desc = "Systemd: Service unit not found"
			cat = "service"
		case 152:
			desc = "Permission denied (EACCES)"
			cat = "permission"
		case 153:
			desc = "Build/compile failed (make/gcc/cmake)"
			cat = "service"
		case 154:
			desc = "Node.js: Native addon build failed (node-gyp)"
			cat = "service"
		// Python / pip / uv
		case 160:
			desc = "Python: Virtualenv / uv environment missing or broken"
			cat = "dependency"
		case 161:
			desc = "Python: Dependency resolution failed"
			cat = "dependency"
		case 162:
			desc = "Python: Installation aborted (EXTERNALLY-MANAGED)"
			cat = "dependency"
		// PostgreSQL
		case 170:
			desc = "PostgreSQL: Connection failed"
			cat = "database"
		case 171:
			desc = "PostgreSQL: Authentication failed"
			cat = "database"
		case 172:
			desc = "PostgreSQL: Database does not exist"
			cat = "database"
		case 173:
			desc = "PostgreSQL: Fatal error in query"
			cat = "database"
		// MySQL / MariaDB
		case 180:
			desc = "MySQL/MariaDB: Connection failed"
			cat = "database"
		case 181:
			desc = "MySQL/MariaDB: Authentication failed"
			cat = "database"
		case 182:
			desc = "MySQL/MariaDB: Database does not exist"
			cat = "database"
		case 183:
			desc = "MySQL/MariaDB: Fatal error in query"
			cat = "database"
		// MongoDB
		case 190:
			desc = "MongoDB: Connection failed"
			cat = "database"
		case 191:
			desc = "MongoDB: Authentication failed"
			cat = "database"
		case 192:
			desc = "MongoDB: Database not found"
			cat = "database"
		case 193:
			desc = "MongoDB: Fatal query error"
			cat = "database"
		// Proxmox Custom Codes
		case 200:
			desc = "Proxmox: Failed to create lock file"
			cat = "proxmox"
		case 203:
			desc = "Proxmox: Missing CTID variable"
			cat = "config"
		case 204:
			desc = "Proxmox: Missing PCT_OSTYPE variable"
			cat = "config"
		case 205:
			desc = "Proxmox: Invalid CTID (<100)"
			cat = "config"
		case 206:
			desc = "Proxmox: CTID already in use"
			cat = "config"
		case 207:
			desc = "Proxmox: Password contains unescaped special chars"
			cat = "config"
		case 208:
			desc = "Proxmox: Invalid configuration (DNS/MAC/Network)"
			cat = "config"
		case 209:
			desc = "Proxmox: Container creation failed"
			cat = "proxmox"
		case 210:
			desc = "Proxmox: Cluster not quorate"
			cat = "proxmox"
		case 211:
			desc = "Proxmox: Timeout waiting for template lock"
			cat = "timeout"
		case 212:
			desc = "Proxmox: Storage 'iscsidirect' does not support containers"
			cat = "proxmox"
		case 213:
			desc = "Proxmox: Storage does not support 'rootdir' content"
			cat = "proxmox"
		case 214:
			desc = "Proxmox: Not enough storage space"
			cat = "storage"
		case 215:
			desc = "Proxmox: Container created but not listed (ghost state)"
			cat = "proxmox"
		case 216:
			desc = "Proxmox: RootFS entry missing in config"
			cat = "proxmox"
		case 217:
			desc = "Proxmox: Storage not accessible"
			cat = "storage"
		case 218:
			desc = "Proxmox: Template file corrupted or incomplete"
			cat = "proxmox"
		case 219:
			desc = "Proxmox: CephFS does not support containers"
			cat = "storage"
		case 220:
			desc = "Proxmox: Unable to resolve template path"
			cat = "proxmox"
		case 221:
			desc = "Proxmox: Template file not readable"
			cat = "proxmox"
		case 222:
			desc = "Proxmox: Template download failed"
			cat = "proxmox"
		case 223:
			desc = "Proxmox: Template not available after download"
			cat = "proxmox"
		case 224:
			desc = "Proxmox: PBS storage is for backups only"
			cat = "storage"
		case 225:
			desc = "Proxmox: No template available for OS/Version"
			cat = "proxmox"
		case 231:
			desc = "Proxmox: LXC stack upgrade failed"
			cat = "proxmox"
		// Node.js / npm
		case 243:
			desc = "Node.js: Out of memory (heap overflow)"
			cat = "resource"
		case 245:
			desc = "Node.js: Invalid command-line option"
			cat = "config"
		case 246:
			desc = "Node.js: Internal JavaScript Parse Error"
			cat = "unknown"
		case 247:
			desc = "Node.js: Fatal internal error"
			cat = "unknown"
		case 248:
			desc = "Node.js: Invalid C++ addon / N-API failure"
			cat = "unknown"
		case 249:
			desc = "npm/pnpm/yarn: Unknown fatal error"
			cat = "unknown"
		case 255:
			desc = "DPKG: Fatal internal error / set -e triggered"
			cat = "apt"
		default:
			if code > 128 && code < 192 {
				sigNum := code - 128
				sigName := ""
				switch sigNum {
				case 1:
					sigName = "SIGHUP (terminal closed)"
				case 2:
					sigName = "SIGINT (user interrupt)"
				case 3:
					sigName = "SIGQUIT (core dump)"
				case 6:
					sigName = "SIGABRT (abort)"
				case 9:
					sigName = "SIGKILL (force killed, likely OOM)"
				case 11:
					sigName = "SIGSEGV (segmentation fault)"
				case 13:
					sigName = "SIGPIPE (broken pipe)"
				case 15:
					sigName = "SIGTERM (terminated)"
				case 24:
					sigName = "SIGXCPU (CPU time limit exceeded)"
				case 25:
					sigName = "SIGXFSZ (file size limit exceeded)"
				default:
					sigName = fmt.Sprintf("signal %d", sigNum)
				}
				desc = "Killed by " + sigName
				cat = "signal"
			} else if code >= 64 && code <= 78 {
				// BSD sysexits.h codes
				switch code {
				case 64:
					desc = "Usage error (wrong arguments)"
				case 65:
					desc = "Data error (bad input data)"
				case 66:
					desc = "Input file not found"
				case 67:
					desc = "User not found"
				case 68:
					desc = "Host not found"
				case 69:
					desc = "Service unavailable"
				case 70:
					desc = "Internal software error"
				case 71:
					desc = "System error (OS error)"
				case 72:
					desc = "Critical OS file missing"
				case 73:
					desc = "Cannot create output file"
				case 74:
					desc = "I/O error"
				case 75:
					desc = "Temporary failure (retry later)"
				case 76:
					desc = "Remote protocol error"
				case 77:
					desc = "Permission denied"
				case 78:
					desc = "Configuration error"
				}
				cat = "unknown"
			}
		}
		pct := float64(count) / float64(data.TotalErrors) * 100
		data.ExitCodeStats = append(data.ExitCodeStats, ExitCodeStat{
			ExitCode:    code,
			Count:       count,
			Description: desc,
			Category:    cat,
			Percentage:  pct,
		})
	}
	// Sort by count desc
	sortExitCodeStats(data.ExitCodeStats)

	// Build category stats
	for cat, count := range categoryCounts {
		apps := categoryApps[cat]
		appList := make([]string, 0, len(apps))
		for a := range apps {
			appList = append(appList, a)
		}
		appsStr := strings.Join(appList, ", ")
		if len(appsStr) > 100 {
			appsStr = appsStr[:97] + "..."
		}

		pct := float64(count) / float64(data.TotalErrors) * 100
		data.CategoryStats = append(data.CategoryStats, CategoryStat{
			Category:   cat,
			Count:      count,
			Percentage: pct,
			TopApps:    appsStr,
		})
	}
	// Sort by count desc
	sortCategoryStats(data.CategoryStats)

	// Build app error details (apps with at least 1 error, sorted by failure count)
	for _, s := range appStats {
		if s.failed+s.aborted == 0 {
			continue
		}
		failRate := float64(s.failed) / float64(s.total) * 100
		data.AppErrors = append(data.AppErrors, AppErrorDetail{
			App:          s.app,
			Type:         s.typ,
			TotalCount:   s.total,
			FailedCount:  s.failed,
			AbortedCount: s.aborted,
			FailureRate:  failRate,
			TopExitCode:  s.topExitCode,
			TopError:     s.topError,
			TopCategory:  s.topCategory,
		})
	}
	sortAppErrors(data.AppErrors)
	if len(data.AppErrors) > 50 {
		data.AppErrors = data.AppErrors[:50]
	}

	// Error timeline
	for i := days - 1; i >= 0; i-- {
		date := time.Now().AddDate(0, 0, -i).Format("2006-01-02")
		data.ErrorTimeline = append(data.ErrorTimeline, ErrorTimelinePoint{
			Date:    date,
			Failed:  dailyFailed[date],
			Aborted: dailyAborted[date],
		})
	}

	data.RecentErrors = recentErrors

	return data, nil
}

type appStatAccum struct {
	app              string
	typ              string
	total            int
	failed           int
	aborted          int
	topExitCode      int
	topExitCodeCount int
	topError         string
	topCategory      string
}

func sortExitCodeStats(s []ExitCodeStat) {
	for i := 0; i < len(s)-1; i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j].Count > s[i].Count {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func sortCategoryStats(s []CategoryStat) {
	for i := 0; i < len(s)-1; i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j].Count > s[i].Count {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func sortAppErrors(s []AppErrorDetail) {
	for i := 0; i < len(s)-1; i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j].FailedCount > s[i].FailedCount {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

// FetchDashboardData retrieves aggregated data from PocketBase
// repoSource filters by repo_source field ("ProxmoxVE", "ProxmoxVED", "external", or "" for all)
func (p *PBClient) FetchDashboardData(ctx context.Context, days int, repoSource string) (*DashboardData, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, err
	}

	data := &DashboardData{}

	// Build filter parts
	var filterParts []string

	// Date filter (days=0 means all entries)
	if days > 0 {
		var since string
		if days == 1 {
			// "Today" = since midnight today (not yesterday)
			since = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			// N days = today + (N-1) previous days
			since = time.Now().AddDate(0, 0, -(days-1)).Format("2006-01-02") + " 00:00:00"
		}
		filterParts = append(filterParts, fmt.Sprintf("created >= '%s'", since))
	}

	// Repo source filter
	if repoSource != "" {
		filterParts = append(filterParts, fmt.Sprintf("repo_source = '%s'", repoSource))
	}

	var filter string
	if len(filterParts) > 0 {
		filter = url.QueryEscape(strings.Join(filterParts, " && "))
	}

	// Fetch all records for the period
	result, err := p.fetchRecords(ctx, filter)
	if err != nil {
		return nil, err
	}
	records := result.Records

	// Set total counts
	data.TotalAllTime = result.TotalItems // Actual total in database
	data.SampleSize = len(records)        // How many we actually processed

	// Aggregate statistics
	appCounts := make(map[string]int)
	osCounts := make(map[string]int)
	methodCounts := make(map[string]int)
	pveCounts := make(map[string]int)
	typeCounts := make(map[string]int)
	errorApps := make(map[string]map[string]bool) // pattern -> set of apps
	errorCounts := make(map[string]int)           // pattern -> total occurrences
	dailySuccess := make(map[string]int)
	dailyFailed := make(map[string]int)

	// Failure tracking per app+type
	appTypeCounts := make(map[string]int)
	appTypeFailures := make(map[string]int)

	// Extended metrics maps
	gpuCounts := make(map[string]int)      // "vendor|passthrough" -> count
	errorCatCounts := make(map[string]int) // category -> count
	toolCounts := make(map[string]int)     // tool_name -> count
	addonCounts := make(map[string]int)    // addon_name -> count
	var totalDuration, durationCount int

	for i := range records {
		r := &records[i]
		data.TotalInstalls++

		// Auto-reclassify: old records still have status="failed" for SIGINT/Ctrl+C/SIGHUP
		if r.Status == "failed" && (r.ExitCode == 129 || r.ExitCode == 130 ||
			strings.Contains(strings.ToLower(r.Error), "sighup") ||
			strings.Contains(strings.ToLower(r.Error), "sigint") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl+c") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl-c")) {
			r.Status = "aborted"
		}

		switch r.Status {
		case "success":
			data.SuccessCount++
		case "failed":
			data.FailedCount++
			// Group errors by pattern
			if r.Error != "" {
				pattern := normalizeError(r.Error)
				errorCounts[pattern]++
				if errorApps[pattern] == nil {
					errorApps[pattern] = make(map[string]bool)
				}
				if r.NSAPP != "" {
					errorApps[pattern][r.NSAPP] = true
				}
			}
		case "aborted":
			data.AbortedCount++
		case "installing", "validation", "configuring":
			data.InstallingCount++
		}

		// Count apps
		if r.NSAPP != "" {
			appCounts[r.NSAPP]++
			// Track per app+type for failure rates
			typeLabel := r.Type
			if typeLabel == "" {
				typeLabel = "unknown"
			}
			ftKey := r.NSAPP + "|" + typeLabel
			appTypeCounts[ftKey]++
			if r.Status == "failed" {
				appTypeFailures[ftKey]++
			}
		}

		// Count OS
		if r.OsType != "" {
			osCounts[r.OsType]++
		}

		// Count methods
		if r.Method != "" {
			methodCounts[r.Method]++
		}

		// Count PVE versions
		if r.PveVer != "" {
			pveCounts[r.PveVer]++
		}

		// Count types (LXC vs VM)
		if r.Type != "" {
			typeCounts[r.Type]++
		}

		// === Extended metrics tracking ===

		// Track PVE tool executions (type="pve", tool name is in nsapp)
		if r.Type == "pve" && r.NSAPP != "" {
			toolCounts[r.NSAPP]++
			data.TotalTools++
		}

		// Track addon installations
		if r.Type == "addon" {
			addonCounts[r.NSAPP]++
			data.TotalAddons++
		}

		// Track GPU usage
		if r.GPUVendor != "" {
			key := r.GPUVendor
			if r.GPUPassthrough != "" {
				key += "|" + r.GPUPassthrough
			}
			gpuCounts[key]++
		}

		// Track error categories
		if r.Status == "failed" && r.ErrorCategory != "" {
			errorCatCounts[r.ErrorCategory]++
		}

		// Track install duration (for averaging)
		if r.InstallDuration > 0 {
			totalDuration += r.InstallDuration
			durationCount++
		}

		// Daily stats (use Created field if available)
		if r.Created != "" {
			date := r.Created[:10] // "2026-02-09"
			if r.Status == "success" {
				dailySuccess[date]++
			} else if r.Status == "failed" {
				dailyFailed[date]++
			}
		}
	}

	// Calculate success rate
	completed := data.SuccessCount + data.FailedCount
	if completed > 0 {
		data.SuccessRate = float64(data.SuccessCount) / float64(completed) * 100
	}

	// Convert maps to sorted slices (increased limits for better analytics)
	data.TopApps = topN(appCounts, 20)
	data.OsDistribution = topNOs(osCounts, 15)
	data.MethodStats = topNMethod(methodCounts, 10)
	data.PveVersions = topNPve(pveCounts, 15)
	data.TypeStats = topNType(typeCounts, 10)

	// Error analysis
	data.ErrorAnalysis = buildErrorAnalysis(errorApps, errorCounts, 15)

	// Failed apps with failure rates - dynamic threshold based on time period
	minInstalls := 10 // default
	switch {
	case days <= 1:
		minInstalls = 5 // Today: need at least 5 installs
	case days <= 7:
		minInstalls = 15 // 7 days: need at least 15 installs
	case days <= 30:
		minInstalls = 40 // 30 days: need at least 40 installs
	case days <= 90:
		minInstalls = 100 // 90 days: need at least 100 installs
	default:
		minInstalls = 100 // 1 year+: need at least 100 installs
	}
	// Returns 16 items: 8 LXC + 8 VM balanced, LXC prioritized
	data.FailedApps = buildFailedApps(appTypeCounts, appTypeFailures, 16, minInstalls)

	// Daily stats for chart
	data.DailyStats = buildDailyStats(dailySuccess, dailyFailed, days)

	// === Extended metrics ===

	// GPU stats
	data.GPUStats = buildGPUStats(gpuCounts)

	// Error categories
	data.ErrorCategories = buildErrorCategories(errorCatCounts)

	// Top tools
	data.TopTools = buildToolStats(toolCounts, 15)

	// Top addons
	data.TopAddons = buildAddonStats(addonCounts, 15)

	// Average install duration
	if durationCount > 0 {
		data.AvgInstallDuration = float64(totalDuration) / float64(durationCount)
	}

	// Recent records (last 20)
	if len(records) > 20 {
		data.RecentRecords = records[:20]
	} else {
		data.RecentRecords = records
	}

	return data, nil
}

// TelemetryRecord includes Created timestamp
type TelemetryRecord struct {
	TelemetryOut
	Created string `json:"created"`
}

// fetchRecordsResult contains records and total count
type fetchRecordsResult struct {
	Records    []TelemetryRecord
	TotalItems int // Actual total in database (not limited)
}

func (p *PBClient) fetchRecords(ctx context.Context, filter string) (*fetchRecordsResult, error) {
	var allRecords []TelemetryRecord
	page := 1
	perPage := 500
	totalItems := 0

	for {
		var reqURL string
		if filter != "" {
			reqURL = fmt.Sprintf("%s/api/collections/%s/records?filter=%s&sort=-created&page=%d&perPage=%d",
				p.baseURL, p.targetColl, filter, page, perPage)
		} else {
			reqURL = fmt.Sprintf("%s/api/collections/%s/records?sort=-created&page=%d&perPage=%d",
				p.baseURL, p.targetColl, page, perPage)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+p.token)

		resp, err := p.http.Do(req)
		if err != nil {
			return nil, err
		}

		var result struct {
			Items      []TelemetryRecord `json:"items"`
			TotalItems int               `json:"totalItems"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		// Store total on first page
		if page == 1 {
			totalItems = result.TotalItems
		}

		allRecords = append(allRecords, result.Items...)

		// Stop when we've fetched all records for the time period
		if len(allRecords) >= result.TotalItems {
			break
		}
		page++
	}

	return &fetchRecordsResult{
		Records:    allRecords,
		TotalItems: totalItems,
	}, nil
}

func topN(m map[string]int, n int) []AppCount {
	result := make([]AppCount, 0, len(m))
	for k, v := range m {
		result = append(result, AppCount{App: k, Count: v})
	}
	// Simple bubble sort for small datasets
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}

func topNOs(m map[string]int, n int) []OsCount {
	result := make([]OsCount, 0, len(m))
	for k, v := range m {
		result = append(result, OsCount{Os: k, Count: v})
	}
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}

func topNMethod(m map[string]int, n int) []MethodCount {
	result := make([]MethodCount, 0, len(m))
	for k, v := range m {
		result = append(result, MethodCount{Method: k, Count: v})
	}
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}

func topNPve(m map[string]int, n int) []PveCount {
	result := make([]PveCount, 0, len(m))
	for k, v := range m {
		result = append(result, PveCount{Version: k, Count: v})
	}
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}

func topNType(m map[string]int, n int) []TypeCount {
	result := make([]TypeCount, 0, len(m))
	for k, v := range m {
		result = append(result, TypeCount{Type: k, Count: v})
	}
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}

// normalizeError simplifies error messages into patterns for grouping
func normalizeError(err string) string {
	err = strings.TrimSpace(err)
	if err == "" {
		return "unknown"
	}

	// Normalize common patterns
	err = strings.ToLower(err)

	// Remove specific numbers, IPs, paths that vary
	// Keep it simple for now - just truncate and normalize
	if len(err) > 60 {
		err = err[:60]
	}

	// Common error pattern replacements
	patterns := map[string]string{
		"connection refused": "connection refused",
		"timeout":            "timeout",
		"no space left":      "disk full",
		"permission denied":  "permission denied",
		"not found":          "not found",
		"failed to download": "download failed",
		"apt":                "apt error",
		"dpkg":               "dpkg error",
		"curl":               "network error",
		"wget":               "network error",
		"docker":             "docker error",
		"systemctl":          "systemd error",
		"service":            "service error",
	}

	for pattern, label := range patterns {
		if strings.Contains(err, pattern) {
			return label
		}
	}

	// If no pattern matches, return first 40 chars
	if len(err) > 40 {
		return err[:40] + "..."
	}
	return err
}

func buildErrorAnalysis(apps map[string]map[string]bool, counts map[string]int, n int) []ErrorGroup {
	result := make([]ErrorGroup, 0, len(apps))

	for pattern, appSet := range apps {
		appList := make([]string, 0, len(appSet))
		for app := range appSet {
			appList = append(appList, app)
		}

		// Limit app list display
		appsStr := strings.Join(appList, ", ")
		if len(appsStr) > 80 {
			appsStr = appsStr[:77] + "..."
		}

		result = append(result, ErrorGroup{
			Pattern:    pattern,
			Count:      counts[pattern],
			UniqueApps: len(appSet),
			Apps:       appsStr,
		})
	}

	// Sort by count descending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	if len(result) > n {
		return result[:n]
	}
	return result
}

func buildFailedApps(total, failed map[string]int, n int, minInstalls int) []AppFailure {
	lxcApps := make([]AppFailure, 0)
	vmApps := make([]AppFailure, 0)

	for key, failCount := range failed {
		totalCount := total[key]
		if totalCount < minInstalls {
			continue // Skip apps with too few installations
		}

		// Parse composite key "app|type"
		parts := strings.SplitN(key, "|", 2)
		app := parts[0]
		appType := ""
		if len(parts) > 1 {
			appType = parts[1]
		}

		rate := float64(failCount) / float64(totalCount) * 100
		failure := AppFailure{
			App:         app,
			Type:        appType,
			TotalCount:  totalCount,
			FailedCount: failCount,
			FailureRate: rate,
		}

		// Separate LXC and VM apps (LXC has higher priority)
		if strings.ToLower(appType) == "lxc" {
			lxcApps = append(lxcApps, failure)
		} else {
			vmApps = append(vmApps, failure)
		}
	}

	// Sort each list by failure rate descending
	sortByFailureRate := func(apps []AppFailure) {
		for i := 0; i < len(apps)-1; i++ {
			for j := i + 1; j < len(apps); j++ {
				if apps[j].FailureRate > apps[i].FailureRate {
					apps[i], apps[j] = apps[j], apps[i]
				}
			}
		}
	}
	sortByFailureRate(lxcApps)
	sortByFailureRate(vmApps)

	// Balance: take equal numbers from each, LXC first
	// n/2 from each type, with LXC getting any extra slot
	perType := n / 2
	extra := n % 2 // Extra slot goes to LXC

	result := make([]AppFailure, 0, n)

	// Take LXC apps first (higher priority)
	lxcCount := perType + extra
	if lxcCount > len(lxcApps) {
		lxcCount = len(lxcApps)
	}
	result = append(result, lxcApps[:lxcCount]...)

	// Take VM apps
	vmCount := perType
	if vmCount > len(vmApps) {
		vmCount = len(vmApps)
	}
	result = append(result, vmApps[:vmCount]...)

	// Fill remaining slots if one type had fewer
	remaining := n - len(result)
	if remaining > 0 {
		// Try to fill with more LXC apps
		extraLxc := len(lxcApps) - lxcCount
		if extraLxc > remaining {
			extraLxc = remaining
		}
		if extraLxc > 0 {
			result = append(result, lxcApps[lxcCount:lxcCount+extraLxc]...)
			remaining -= extraLxc
		}
		// Fill with more VM apps if still slots available
		if remaining > 0 {
			extraVm := len(vmApps) - vmCount
			if extraVm > remaining {
				extraVm = remaining
			}
			if extraVm > 0 {
				result = append(result, vmApps[vmCount:vmCount+extraVm]...)
			}
		}
	}

	return result
}

func buildDailyStats(success, failed map[string]int, days int) []DailyStat {
	result := make([]DailyStat, 0, days)
	for i := days - 1; i >= 0; i-- {
		date := time.Now().AddDate(0, 0, -i).Format("2006-01-02")
		result = append(result, DailyStat{
			Date:    date,
			Success: success[date],
			Failed:  failed[date],
		})
	}
	return result
}

// === Extended metrics helper functions ===

func buildGPUStats(gpuCounts map[string]int) []GPUCount {
	result := make([]GPUCount, 0, len(gpuCounts))
	for key, count := range gpuCounts {
		parts := strings.Split(key, "|")
		vendor := parts[0]
		passthrough := ""
		if len(parts) > 1 {
			passthrough = parts[1]
		}
		result = append(result, GPUCount{
			Vendor:      vendor,
			Passthrough: passthrough,
			Count:       count,
		})
	}
	// Sort by count descending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

func buildErrorCategories(catCounts map[string]int) []ErrorCatCount {
	result := make([]ErrorCatCount, 0, len(catCounts))
	for cat, count := range catCounts {
		result = append(result, ErrorCatCount{
			Category: cat,
			Count:    count,
		})
	}
	// Sort by count descending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

func buildToolStats(toolCounts map[string]int, n int) []ToolCount {
	result := make([]ToolCount, 0, len(toolCounts))
	for tool, count := range toolCounts {
		result = append(result, ToolCount{
			Tool:  tool,
			Count: count,
		})
	}
	// Sort by count descending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}

func buildAddonStats(addonCounts map[string]int, n int) []AddonCount {
	result := make([]AddonCount, 0, len(addonCounts))
	for addon, count := range addonCounts {
		result = append(result, AddonCount{
			Addon: addon,
			Count: count,
		})
	}
	// Sort by count descending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Count > result[i].Count {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	if len(result) > n {
		return result[:n]
	}
	return result
}
