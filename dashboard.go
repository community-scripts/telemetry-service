package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
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

// FetchScriptStatsFromCollection reads pre-computed stats from a PocketBase
// collection that is populated by SQL views/triggers (e.g. _script_stats_7d).
func (p *PBClient) FetchScriptStatsFromCollection(ctx context.Context, collection string, windowDays int) (*ScriptAnalysisData, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, err
	}

	knownScripts, _ := p.FetchKnownScripts(ctx)

	// Fetch all rows from the stats collection
	type statsRow struct {
		Slug       string `json:"slug"`
		Type       string `json:"type"`
		Total      int    `json:"total"`
		Success    int    `json:"success"`
		Failed     int    `json:"failed"`
		Aborted    int    `json:"aborted"`
		Installing int    `json:"installing"`
	}

	page := 1
	perPage := 500
	var rows []statsRow
	for {
		reqURL := fmt.Sprintf("%s/api/collections/%s/records?page=%d&perPage=%d",
			p.baseURL, collection, page, perPage)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+p.token)

		resp, err := p.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch %s failed: %w", collection, err)
		}
		var result struct {
			Items      []statsRow `json:"items"`
			TotalItems int        `json:"totalItems"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		rows = append(rows, result.Items...)
		if len(rows) >= result.TotalItems || len(result.Items) == 0 {
			break
		}
		page++
	}

	// Build ScriptAnalysisData from the SQL-generated stats
	data := &ScriptAnalysisData{}
	now := time.Now()
	seen := make(map[string]bool)

	for _, st := range rows {
		if st.Total == 0 {
			continue
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
	if knownScripts != nil && (windowDays >= 30 || windowDays == 0) {
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

	log.Printf("[STATS] Loaded %d script stats from %s", len(rows), collection)
	return data, nil
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
		if code == 0 {
			// exit_code=0 is Success — skip from error stats
			continue
		}
		desc := getExitCodeDescription(code)
		cat := getExitCodeCategory(code)
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
	perPage := 1000
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
