package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DashboardData holds aggregated statistics for the dashboard
type DashboardData struct {
	TotalInstalls   int               `json:"total_installs"`
	SuccessCount    int               `json:"success_count"`
	FailedCount     int               `json:"failed_count"`
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
	GPUStats           []GPUCount       `json:"gpu_stats"`
	ErrorCategories    []ErrorCatCount  `json:"error_categories"`
	TopTools           []ToolCount      `json:"top_tools"`
	TopAddons          []AddonCount     `json:"top_addons"`
	AvgInstallDuration float64          `json:"avg_install_duration"` // seconds
	TotalTools         int              `json:"total_tools"`
	TotalAddons        int              `json:"total_addons"`
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
	Pattern string `json:"pattern"`
	Count   int    `json:"count"`
	Apps    string `json:"apps"` // Comma-separated list of affected apps
}

type AppFailure struct {
	App         string  `json:"app"`
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
	Vendor     string `json:"vendor"`
	Passthrough string `json:"passthrough"`
	Count      int    `json:"count"`
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
	Addon    string `json:"addon"`
	ParentCT string `json:"parent_ct"`
	Count    int    `json:"count"`
}

// FetchDashboardData retrieves aggregated data from PocketBase
func (p *PBClient) FetchDashboardData(ctx context.Context, days int) (*DashboardData, error) {
	if err := p.ensureAuth(ctx); err != nil {
		return nil, err
	}

	data := &DashboardData{}

	// Calculate date filter
	since := time.Now().AddDate(0, 0, -days).Format("2006-01-02 00:00:00")
	filter := url.QueryEscape(fmt.Sprintf("created >= '%s'", since))

	// Fetch all records for the period
	records, err := p.fetchRecords(ctx, filter, 500)
	if err != nil {
		return nil, err
	}

	// Aggregate statistics
	appCounts := make(map[string]int)
	appFailures := make(map[string]int)
	osCounts := make(map[string]int)
	methodCounts := make(map[string]int)
	pveCounts := make(map[string]int)
	typeCounts := make(map[string]int)
	errorPatterns := make(map[string]map[string]bool) // pattern -> set of apps
	dailySuccess := make(map[string]int)
	dailyFailed := make(map[string]int)

	// Extended metrics maps
	gpuCounts := make(map[string]int)              // "vendor|passthrough" -> count
	errorCatCounts := make(map[string]int)          // category -> count
	toolCounts := make(map[string]int)              // tool_name -> count
	addonCounts := make(map[string]int)             // addon_name -> count
	var totalDuration, durationCount int

	for _, r := range records {
		data.TotalInstalls++

		switch r.Status {
		case "success":
			data.SuccessCount++
		case "failed":
			data.FailedCount++
			// Track failed apps
			if r.NSAPP != "" {
				appFailures[r.NSAPP]++
			}
			// Group errors by pattern
			if r.Error != "" {
				pattern := normalizeError(r.Error)
				if errorPatterns[pattern] == nil {
					errorPatterns[pattern] = make(map[string]bool)
				}
				if r.NSAPP != "" {
					errorPatterns[pattern][r.NSAPP] = true
				}
			}
		case "installing":
			data.InstallingCount++
		}

		// Count apps
		if r.NSAPP != "" {
			appCounts[r.NSAPP]++
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

		// Track tool executions
		if r.Type == "tool" && r.ToolName != "" {
			toolCounts[r.ToolName]++
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

	// Convert maps to sorted slices (top 10)
	data.TopApps = topN(appCounts, 10)
	data.OsDistribution = topNOs(osCounts, 10)
	data.MethodStats = topNMethod(methodCounts, 10)
	data.PveVersions = topNPve(pveCounts, 10)
	data.TypeStats = topNType(typeCounts, 10)

	// Error analysis
	data.ErrorAnalysis = buildErrorAnalysis(errorPatterns, 10)

	// Failed apps with failure rates
	data.FailedApps = buildFailedApps(appCounts, appFailures, 10)

	// Daily stats for chart
	data.DailyStats = buildDailyStats(dailySuccess, dailyFailed, days)

	// === Extended metrics ===

	// GPU stats
	data.GPUStats = buildGPUStats(gpuCounts)

	// Error categories
	data.ErrorCategories = buildErrorCategories(errorCatCounts)

	// Top tools
	data.TopTools = buildToolStats(toolCounts, 10)

	// Top addons
	data.TopAddons = buildAddonStats(addonCounts, 10)

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

func (p *PBClient) fetchRecords(ctx context.Context, filter string, limit int) ([]TelemetryRecord, error) {
	var allRecords []TelemetryRecord
	page := 1
	perPage := 100

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/api/collections/%s/records?filter=%s&sort=-created&page=%d&perPage=%d",
				p.baseURL, p.targetColl, filter, page, perPage),
			nil,
		)
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

		allRecords = append(allRecords, result.Items...)

		if len(allRecords) >= limit || len(allRecords) >= result.TotalItems {
			break
		}
		page++
	}

	return allRecords, nil
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
		"connection refused":  "connection refused",
		"timeout":             "timeout",
		"no space left":       "disk full",
		"permission denied":   "permission denied",
		"not found":           "not found",
		"failed to download":  "download failed",
		"apt":                 "apt error",
		"dpkg":                "dpkg error",
		"curl":                "network error",
		"wget":                "network error",
		"docker":              "docker error",
		"systemctl":           "systemd error",
		"service":             "service error",
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

func buildErrorAnalysis(patterns map[string]map[string]bool, n int) []ErrorGroup {
	result := make([]ErrorGroup, 0, len(patterns))

	for pattern, apps := range patterns {
		appList := make([]string, 0, len(apps))
		for app := range apps {
			appList = append(appList, app)
		}

		// Limit app list display
		appsStr := strings.Join(appList, ", ")
		if len(appsStr) > 50 {
			appsStr = appsStr[:47] + "..."
		}

		result = append(result, ErrorGroup{
			Pattern: pattern,
			Count:   len(apps), // Number of unique apps with this error
			Apps:    appsStr,
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

func buildFailedApps(total, failed map[string]int, n int) []AppFailure {
	result := make([]AppFailure, 0)

	for app, failCount := range failed {
		totalCount := total[app]
		if totalCount == 0 {
			continue
		}

		rate := float64(failCount) / float64(totalCount) * 100
		result = append(result, AppFailure{
			App:         app,
			TotalCount:  totalCount,
			FailedCount: failCount,
			FailureRate: rate,
		})
	}

	// Sort by failure rate descending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].FailureRate > result[i].FailureRate {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	if len(result) > n {
		return result[:n]
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

// DashboardHTML returns the embedded dashboard HTML
func DashboardHTML() string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Telemetry Dashboard - ProxmoxVE Helper Scripts</title>
    <meta name="description" content="Installation telemetry dashboard for ProxmoxVE Helper Scripts">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📊</text></svg>">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --border-color: #30363d;
            --text-primary: #c9d1d9;
            --text-secondary: #8b949e;
            --accent-blue: #58a6ff;
            --accent-green: #3fb950;
            --accent-red: #f85149;
            --accent-yellow: #d29922;
            --accent-purple: #a371f7;
        }
        
        [data-theme="light"] {
            --bg-primary: #ffffff;
            --bg-secondary: #f6f8fa;
            --bg-tertiary: #eaeef2;
            --border-color: #d0d7de;
            --text-primary: #1f2328;
            --text-secondary: #656d76;
            --accent-blue: #0969da;
            --accent-green: #1a7f37;
            --accent-red: #cf222e;
            --accent-yellow: #9a6700;
            --accent-purple: #8250df;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
        }
        
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .header h1 {
            font-size: 24px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .header h1 img {
            height: 40px;
        }
        
        .controls {
            display: flex;
            gap: 12px;
            align-items: center;
        }
        
        select, button {
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 8px 16px;
            border-radius: 6px;
            font-size: 14px;
            cursor: pointer;
        }
        
        select:hover, button:hover {
            border-color: var(--accent-blue);
        }
        
        button {
            background: var(--accent-blue);
            border-color: var(--accent-blue);
            color: #fff;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        
        .stat-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
        }
        
        .stat-card .label {
            font-size: 12px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        
        .stat-card .value {
            font-size: 32px;
            font-weight: 600;
        }
        
        .stat-card .value.success { color: var(--accent-green); }
        .stat-card .value.failed { color: var(--accent-red); }
        .stat-card .value.pending { color: var(--accent-yellow); }
        .stat-card .value.rate { color: var(--accent-blue); }
        
        .charts-grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 16px;
            margin-bottom: 24px;
        }
        
        @media (max-width: 1200px) {
            .charts-grid {
                grid-template-columns: 1fr;
            }
        }
        
        .chart-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
        }
        
        .chart-card h3 {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 16px;
            color: var(--text-secondary);
        }
        
        .chart-container {
            position: relative;
            height: 300px;
        }
        
        .small-charts {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        
        .table-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            overflow: hidden;
        }
        
        .table-card h3 {
            font-size: 14px;
            font-weight: 600;
            padding: 16px 20px;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-secondary);
        }
        
        .filters {
            padding: 12px 20px;
            background: var(--bg-tertiary);
            border-bottom: 1px solid var(--border-color);
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
        }
        
        .filters input, .filters select {
            background: var(--bg-secondary);
            flex: 1;
            min-width: 150px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
        }
        
        th, td {
            padding: 12px 20px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }
        
        th {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            background: var(--bg-tertiary);
        }
        
        td {
            font-size: 14px;
        }
        
        tr:hover {
            background: var(--bg-tertiary);
        }
        
        .status-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }
        
        .status-badge.success { background: rgba(63, 185, 80, 0.2); color: var(--accent-green); }
        .status-badge.failed { background: rgba(248, 81, 73, 0.2); color: var(--accent-red); }
        .status-badge.installing { background: rgba(210, 153, 34, 0.2); color: var(--accent-yellow); }
        
        .loading {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 200px;
            color: var(--text-secondary);
        }
        
        .error {
            background: rgba(248, 81, 73, 0.1);
            border: 1px solid var(--accent-red);
            color: var(--accent-red);
            padding: 16px;
            border-radius: 8px;
            margin-bottom: 24px;
        }
        
        .last-updated {
            font-size: 12px;
            color: var(--text-secondary);
        }
        
        .footer {
            margin-top: 24px;
            padding-top: 16px;
            border-top: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            color: var(--text-secondary);
            font-size: 12px;
        }
        
        .footer a {
            color: var(--accent-blue);
            text-decoration: none;
        }
        
        .footer a:hover {
            text-decoration: underline;
        }
        
        .export-btn {
            background: var(--bg-tertiary);
            border-color: var(--border-color);
            color: var(--text-primary);
        }
        
        .export-btn:hover {
            background: var(--bg-secondary);
        }
        
        .pve-version-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 24px;
        }
        
        .pve-version-card h3 {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 12px;
            color: var(--text-secondary);
        }
        
        .pve-versions {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }
        
        .pve-badge {
            background: var(--bg-tertiary);
            padding: 6px 12px;
            border-radius: 16px;
            font-size: 12px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .pve-badge .count {
            background: var(--accent-purple);
            color: #fff;
            padding: 2px 6px;
            border-radius: 10px;
            font-size: 10px;
        }
        
        .theme-toggle {
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 8px 12px;
            border-radius: 6px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .theme-toggle:hover {
            border-color: var(--accent-blue);
        }
        
        .error-analysis-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 24px;
        }
        
        .error-analysis-card h3 {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 12px;
            color: var(--text-secondary);
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .error-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .error-item {
            background: var(--bg-tertiary);
            border-radius: 6px;
            padding: 12px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .error-item .pattern {
            font-family: monospace;
            color: var(--accent-red);
            font-size: 13px;
        }
        
        .error-item .meta {
            font-size: 12px;
            color: var(--text-secondary);
        }
        
        .error-item .count-badge {
            background: var(--accent-red);
            color: #fff;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        
        .failed-apps-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 12px;
            margin-top: 12px;
        }
        
        .failed-app-card {
            background: var(--bg-tertiary);
            border-radius: 6px;
            padding: 12px;
        }
        
        .failed-app-card .app-name {
            font-weight: 600;
            margin-bottom: 4px;
        }
        
        .failed-app-card .failure-rate {
            font-size: 20px;
            font-weight: 600;
            color: var(--accent-red);
        }
        
        .failed-app-card .details {
            font-size: 11px;
            color: var(--text-secondary);
        }
        
        .pagination {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 8px;
            padding: 16px;
        }
        
        .pagination button {
            padding: 6px 12px;
        }
        
        .pagination span {
            color: var(--text-secondary);
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M3 3v18h18"/>
                <path d="M18.7 8l-5.1 5.2-2.8-2.7L7 14.3"/>
            </svg>
            Telemetry Dashboard
        </h1>
        <div class="controls">
            <select id="timeRange">
                <option value="7">Last 7 days</option>
                <option value="14">Last 14 days</option>
                <option value="30" selected>Last 30 days</option>
                <option value="90">Last 90 days</option>
                <option value="365">Last year</option>
            </select>
            <button class="export-btn" onclick="exportCSV()">Export CSV</button>
            <button onclick="refreshData()">Refresh</button>
            <button class="theme-toggle" onclick="toggleTheme()">
                <span id="themeIcon">🌙</span>
            </button>
            <span class="last-updated" id="lastUpdated"></span>
        </div>
    </div>
    
    <div id="error" class="error" style="display: none;"></div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="label">Total Installations</div>
            <div class="value" id="totalInstalls">-</div>
        </div>
        <div class="stat-card">
            <div class="label">Successful</div>
            <div class="value success" id="successCount">-</div>
        </div>
        <div class="stat-card">
            <div class="label">Failed</div>
            <div class="value failed" id="failedCount">-</div>
        </div>
        <div class="stat-card">
            <div class="label">In Progress</div>
            <div class="value pending" id="installingCount">-</div>
        </div>
        <div class="stat-card">
            <div class="label">Success Rate</div>
            <div class="value rate" id="successRate">-</div>
        </div>
        <div class="stat-card">
            <div class="label">LXC / VM</div>
            <div class="value" id="typeStats" style="font-size: 20px;">-</div>
        </div>
    </div>
    
    <div class="pve-version-card">
        <h3>Proxmox VE Versions</h3>
        <div class="pve-versions" id="pveVersions">
            <span class="loading">Loading...</span>
        </div>
    </div>
    
    <div class="charts-grid">
        <div class="chart-card">
            <h3>Installations Over Time</h3>
            <div class="chart-container">
                <canvas id="dailyChart"></canvas>
            </div>
        </div>
        <div class="chart-card">
            <h3>Status Distribution</h3>
            <div class="chart-container">
                <canvas id="statusChart"></canvas>
            </div>
        </div>
    </div>
    
    <div class="small-charts">
        <div class="chart-card">
            <h3>Top Applications</h3>
            <div class="chart-container">
                <canvas id="appsChart"></canvas>
            </div>
        </div>
        <div class="chart-card">
            <h3>OS Distribution</h3>
            <div class="chart-container">
                <canvas id="osChart"></canvas>
            </div>
        </div>
        <div class="chart-card">
            <h3>Installation Method</h3>
            <div class="chart-container">
                <canvas id="methodChart"></canvas>
            </div>
        </div>
    </div>
    
    <div class="error-analysis-card">
        <h3>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"/>
                <line x1="12" y1="8" x2="12" y2="12"/>
                <line x1="12" y1="16" x2="12.01" y2="16"/>
            </svg>
            Error Analysis
        </h3>
        <div class="error-list" id="errorList">
            <span class="loading">Loading...</span>
        </div>
    </div>
    
    <div class="error-analysis-card">
        <h3>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
                <line x1="12" y1="9" x2="12" y2="13"/>
                <line x1="12" y1="17" x2="12.01" y2="17"/>
            </svg>
            Apps with Highest Failure Rates
        </h3>
        <div class="failed-apps-grid" id="failedAppsGrid">
            <span class="loading">Loading...</span>
        </div>
    </div>
    
    <div class="table-card">
        <h3>Recent Installations</h3>
        <div class="filters">
            <input type="text" id="filterApp" placeholder="Filter by app..." oninput="filterTable()">
            <select id="filterStatus" onchange="filterTable()">
                <option value="">All Status</option>
                <option value="success">Success</option>
                <option value="failed">Failed</option>
                <option value="installing">Installing</option>
            </select>
            <select id="filterOs" onchange="filterTable()">
                <option value="">All OS</option>
            </select>
        </div>
        <table>
            <thead>
                <tr>
                    <th>App</th>
                    <th>Status</th>
                    <th>OS</th>
                    <th>Type</th>
                    <th>Method</th>
                    <th>Resources</th>
                    <th>Exit Code</th>
                    <th>Error</th>
                </tr>
            </thead>
            <tbody id="recordsTable">
                <tr><td colspan="8" class="loading">Loading...</td></tr>
            </tbody>
        </table>
        <div class="pagination">
            <button onclick="prevPage()" id="prevBtn" disabled>← Previous</button>
            <span id="pageInfo">Page 1</span>
            <button onclick="nextPage()" id="nextBtn">Next →</button>
        </div>
    </div>
    
    <div class="footer">
        <div>
            <a href="https://github.com/community-scripts/ProxmoxVED" target="_blank">ProxmoxVE Helper Scripts</a> 
            &bull; Telemetry is anonymous and privacy-friendly
        </div>
        <div>
            <a href="/healthz" target="_blank">Health Check</a> &bull;
            <a href="/metrics" target="_blank">Metrics</a> &bull;
            <a href="/api/dashboard" target="_blank">API</a>
        </div>
    </div>
    
    <script>
        let charts = {};
        let allRecords = [];
        let currentPage = 1;
        let totalPages = 1;
        let currentTheme = localStorage.getItem('theme') || 'dark';
        
        // Apply saved theme on load
        if (currentTheme === 'light') {
            document.documentElement.setAttribute('data-theme', 'light');
            document.getElementById('themeIcon').textContent = '☀️';
        }
        
        function toggleTheme() {
            if (currentTheme === 'dark') {
                document.documentElement.setAttribute('data-theme', 'light');
                document.getElementById('themeIcon').textContent = '☀️';
                currentTheme = 'light';
            } else {
                document.documentElement.removeAttribute('data-theme');
                document.getElementById('themeIcon').textContent = '🌙';
                currentTheme = 'dark';
            }
            localStorage.setItem('theme', currentTheme);
            // Redraw charts with new colors
            if (Object.keys(charts).length > 0) {
                refreshData();
            }
        }
        
        const chartColors = {
            blue: 'rgba(88, 166, 255, 0.8)',
            green: 'rgba(63, 185, 80, 0.8)',
            red: 'rgba(248, 81, 73, 0.8)',
            yellow: 'rgba(210, 153, 34, 0.8)',
            purple: 'rgba(163, 113, 247, 0.8)',
            gray: 'rgba(139, 148, 158, 0.8)'
        };
        
        const chartDefaults = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: { color: '#c9d1d9' }
                }
            },
            scales: {
                x: {
                    ticks: { color: '#8b949e' },
                    grid: { color: '#30363d' }
                },
                y: {
                    ticks: { color: '#8b949e' },
                    grid: { color: '#30363d' }
                }
            }
        };
        
        async function fetchData() {
            const days = document.getElementById('timeRange').value;
            try {
                const response = await fetch('/api/dashboard?days=' + days);
                if (!response.ok) throw new Error('Failed to fetch data');
                return await response.json();
            } catch (error) {
                document.getElementById('error').style.display = 'block';
                document.getElementById('error').textContent = 'Error: ' + error.message;
                throw error;
            }
        }
        
        function updateStats(data) {
            document.getElementById('totalInstalls').textContent = data.total_installs.toLocaleString();
            document.getElementById('successCount').textContent = data.success_count.toLocaleString();
            document.getElementById('failedCount').textContent = data.failed_count.toLocaleString();
            document.getElementById('installingCount').textContent = data.installing_count.toLocaleString();
            document.getElementById('successRate').textContent = data.success_rate.toFixed(1) + '%';
            document.getElementById('lastUpdated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
            document.getElementById('error').style.display = 'none';
            
            // Type stats (LXC/VM)
            if (data.type_stats && data.type_stats.length > 0) {
                const lxc = data.type_stats.find(t => t.type === 'lxc');
                const vm = data.type_stats.find(t => t.type === 'vm');
                document.getElementById('typeStats').textContent = 
                    (lxc ? lxc.count.toLocaleString() : '0') + ' / ' + (vm ? vm.count.toLocaleString() : '0');
            }
            
            // PVE Versions
            if (data.pve_versions && data.pve_versions.length > 0) {
                document.getElementById('pveVersions').innerHTML = data.pve_versions.map(p => 
                    '<span class="pve-badge">PVE ' + (p.version || 'unknown') + ' <span class="count">' + p.count + '</span></span>'
                ).join('');
            } else {
                document.getElementById('pveVersions').innerHTML = '<span>No version data</span>';
            }
            
            // Error Analysis
            updateErrorAnalysis(data.error_analysis || []);
            
            // Failed Apps
            updateFailedApps(data.failed_apps || []);
        }
        
        function updateErrorAnalysis(errors) {
            const container = document.getElementById('errorList');
            if (!errors || errors.length === 0) {
                container.innerHTML = '<span class="loading">No errors recorded</span>';
                return;
            }
            
            container.innerHTML = errors.slice(0, 8).map(e => 
                '<div class="error-item">' +
                    '<div>' +
                        '<div class="pattern">' + escapeHtml(e.pattern) + '</div>' +
                        '<div class="meta">Affects: ' + escapeHtml(e.apps) + '</div>' +
                    '</div>' +
                    '<span class="count-badge">' + e.count + ' apps</span>' +
                '</div>'
            ).join('');
        }
        
        function updateFailedApps(apps) {
            const container = document.getElementById('failedAppsGrid');
            if (!apps || apps.length === 0) {
                container.innerHTML = '<span class="loading">No failures recorded</span>';
                return;
            }
            
            container.innerHTML = apps.slice(0, 8).map(a => 
                '<div class="failed-app-card">' +
                    '<div class="app-name">' + escapeHtml(a.app) + '</div>' +
                    '<div class="failure-rate">' + a.failure_rate.toFixed(1) + '%</div>' +
                    '<div class="details">' + a.failed_count + ' / ' + a.total_count + ' failed</div>' +
                '</div>'
            ).join('');
        }
        
        function escapeHtml(str) {
            if (!str) return '';
            return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }
        
        function updateCharts(data) {
            // Daily chart
            if (charts.daily) charts.daily.destroy();
            charts.daily = new Chart(document.getElementById('dailyChart'), {
                type: 'line',
                data: {
                    labels: data.daily_stats.map(d => d.date.slice(5)), // MM-DD
                    datasets: [
                        {
                            label: 'Success',
                            data: data.daily_stats.map(d => d.success),
                            borderColor: chartColors.green,
                            backgroundColor: 'rgba(63, 185, 80, 0.1)',
                            fill: true,
                            tension: 0.3
                        },
                        {
                            label: 'Failed',
                            data: data.daily_stats.map(d => d.failed),
                            borderColor: chartColors.red,
                            backgroundColor: 'rgba(248, 81, 73, 0.1)',
                            fill: true,
                            tension: 0.3
                        }
                    ]
                },
                options: chartDefaults
            });
            
            // Status pie chart
            if (charts.status) charts.status.destroy();
            charts.status = new Chart(document.getElementById('statusChart'), {
                type: 'doughnut',
                data: {
                    labels: ['Success', 'Failed', 'Installing'],
                    datasets: [{
                        data: [data.success_count, data.failed_count, data.installing_count],
                        backgroundColor: [chartColors.green, chartColors.red, chartColors.yellow]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: { color: '#c9d1d9' }
                        }
                    }
                }
            });
            
            // Top apps chart
            if (charts.apps) charts.apps.destroy();
            charts.apps = new Chart(document.getElementById('appsChart'), {
                type: 'bar',
                data: {
                    labels: data.top_apps.map(a => a.app),
                    datasets: [{
                        label: 'Installations',
                        data: data.top_apps.map(a => a.count),
                        backgroundColor: chartColors.blue
                    }]
                },
                options: {
                    ...chartDefaults,
                    indexAxis: 'y',
                    plugins: { legend: { display: false } }
                }
            });
            
            // OS distribution chart
            if (charts.os) charts.os.destroy();
            charts.os = new Chart(document.getElementById('osChart'), {
                type: 'pie',
                data: {
                    labels: data.os_distribution.map(o => o.os),
                    datasets: [{
                        data: data.os_distribution.map(o => o.count),
                        backgroundColor: [
                            chartColors.blue, chartColors.green, chartColors.purple,
                            chartColors.yellow, chartColors.red, chartColors.gray
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'right',
                            labels: { color: '#c9d1d9' }
                        }
                    }
                }
            });
            
            // Method chart
            if (charts.method) charts.method.destroy();
            charts.method = new Chart(document.getElementById('methodChart'), {
                type: 'bar',
                data: {
                    labels: data.method_stats.map(m => m.method || 'default'),
                    datasets: [{
                        label: 'Count',
                        data: data.method_stats.map(m => m.count),
                        backgroundColor: chartColors.purple
                    }]
                },
                options: {
                    ...chartDefaults,
                    plugins: { legend: { display: false } }
                }
            });
        }
        
        function updateTable(records) {
            allRecords = records || [];
            
            // Populate OS filter
            const osFilter = document.getElementById('filterOs');
            const uniqueOs = [...new Set(allRecords.map(r => r.os_type).filter(Boolean))];
            osFilter.innerHTML = '<option value="">All OS</option>' + 
                uniqueOs.map(os => '<option value="' + os + '">' + os + '</option>').join('');
            
            filterTable();
        }
        
        async function fetchPaginatedRecords() {
            const status = document.getElementById('filterStatus').value;
            const app = document.getElementById('filterApp').value;
            const os = document.getElementById('filterOs').value;
            
            try {
                let url = '/api/records?page=' + currentPage + '&limit=50';
                if (status) url += '&status=' + encodeURIComponent(status);
                if (app) url += '&app=' + encodeURIComponent(app);
                if (os) url += '&os=' + encodeURIComponent(os);
                
                const response = await fetch(url);
                if (!response.ok) throw new Error('Failed to fetch records');
                const data = await response.json();
                
                totalPages = data.total_pages || 1;
                document.getElementById('pageInfo').textContent = 'Page ' + currentPage + ' of ' + totalPages + ' (' + data.total + ' total)';
                document.getElementById('prevBtn').disabled = currentPage <= 1;
                document.getElementById('nextBtn').disabled = currentPage >= totalPages;
                
                renderTableRows(data.records || []);
            } catch (e) {
                console.error('Pagination error:', e);
            }
        }
        
        function prevPage() {
            if (currentPage > 1) {
                currentPage--;
                fetchPaginatedRecords();
            }
        }
        
        function nextPage() {
            if (currentPage < totalPages) {
                currentPage++;
                fetchPaginatedRecords();
            }
        }
        
        function renderTableRows(records) {
            const tbody = document.getElementById('recordsTable');
            if (records.length === 0) {
                tbody.innerHTML = '<tr><td colspan="8" class="loading">No records found</td></tr>';
                return;
            }
            
            tbody.innerHTML = records.map(r => {
                const statusClass = r.status || 'unknown';
                const resources = r.core_count || r.ram_size || r.disk_size 
                    ? (r.core_count || '?') + 'C / ' + (r.ram_size ? Math.round(r.ram_size/1024) + 'G' : '?') + ' / ' + (r.disk_size || '?') + 'GB'
                    : '-';
                return '<tr>' +
                    '<td><strong>' + escapeHtml(r.nsapp || '-') + '</strong></td>' +
                    '<td><span class="status-badge ' + statusClass + '">' + escapeHtml(r.status || '-') + '</span></td>' +
                    '<td>' + escapeHtml(r.os_type || '-') + ' ' + escapeHtml(r.os_version || '') + '</td>' +
                    '<td>' + escapeHtml(r.type || '-') + '</td>' +
                    '<td>' + escapeHtml(r.method || 'default') + '</td>' +
                    '<td>' + resources + '</td>' +
                    '<td>' + (r.exit_code || '-') + '</td>' +
                    '<td title="' + escapeHtml(r.error || '') + '">' + 
                        escapeHtml((r.error || '').slice(0, 40)) + (r.error && r.error.length > 40 ? '...' : '') + '</td>' +
                '</tr>';
            }).join('');
        }
        
        function filterTable() {
            currentPage = 1;
            fetchPaginatedRecords();
        }
        
        function exportCSV() {
            if (allRecords.length === 0) {
                alert('No data to export');
                return;
            }
            
            const headers = ['App', 'Status', 'OS Type', 'OS Version', 'Type', 'Method', 'Cores', 'RAM (MB)', 'Disk (GB)', 'Exit Code', 'Error', 'PVE Version'];
            const rows = allRecords.map(r => [
                r.nsapp || '',
                r.status || '',
                r.os_type || '',
                r.os_version || '',
                r.type || '',
                r.method || '',
                r.core_count || '',
                r.ram_size || '',
                r.disk_size || '',
                r.exit_code || '',
                (r.error || '').replace(/,/g, ';'),
                r.pve_version || ''
            ]);
            
            const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\\n');
            const blob = new Blob([csv], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'telemetry_' + new Date().toISOString().slice(0,10) + '.csv';
            a.click();
            URL.revokeObjectURL(url);
        }
        
        async function refreshData() {
            try {
                const data = await fetchData();
                updateStats(data);
                updateCharts(data);
                updateTable(data.recent_records);
            } catch (e) {
                console.error(e);
            }
        }
        
        // Initial load
        refreshData();
        
        // Refresh on time range change
        document.getElementById('timeRange').addEventListener('change', refreshData);
        
        // Auto-refresh every 60 seconds
        setInterval(refreshData, 60000);
    </script>
</body>
</html>`
}