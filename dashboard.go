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
	TotalAllTime    int               `json:"total_all_time"`    // Total records in DB (not limited)
	SampleSize      int               `json:"sample_size"`       // How many records were sampled
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
	Addon string `json:"addon"`
	Count int    `json:"count"`
}

// ========================================================
// Error Analysis Data Types
// ========================================================

// ErrorAnalysisData holds comprehensive error analysis
type ErrorAnalysisData struct {
	TotalErrors       int                  `json:"total_errors"`
	TotalInstalls     int                  `json:"total_installs"`
	OverallFailRate   float64              `json:"overall_fail_rate"`
	ExitCodeStats     []ExitCodeStat       `json:"exit_code_stats"`
	CategoryStats     []CategoryStat       `json:"category_stats"`
	AppErrors         []AppErrorDetail     `json:"app_errors"`
	RecentErrors      []ErrorRecord        `json:"recent_errors"`
	StuckInstalling   int                  `json:"stuck_installing"`
	ErrorTimeline     []ErrorTimelinePoint `json:"error_timeline"`
}

type ExitCodeStat struct {
	ExitCode    int    `json:"exit_code"`
	Count       int    `json:"count"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Percentage  float64 `json:"percentage"`
}

type CategoryStat struct {
	Category    string  `json:"category"`
	Count       int     `json:"count"`
	Percentage  float64 `json:"percentage"`
	TopApps     string  `json:"top_apps"`
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
			since = time.Now().AddDate(0, 0, -(days - 1)).Format("2006-01-02") + " 00:00:00"
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

		// Auto-reclassify (same logic as dashboard)
		if r.Status == "failed" && (r.ExitCode == 130 ||
			strings.Contains(strings.ToLower(r.Error), "sigint") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl+c") ||
			strings.Contains(strings.ToLower(r.Error), "ctrl-c") ||
			strings.Contains(strings.ToLower(r.Error), "aborted by user") ||
			strings.Contains(strings.ToLower(r.Error), "no changes have been made")) {
			r.Status = "aborted"
		}

		// Reclassify: status="failed" with exit_code=0 and no error text is actually success
		if r.Status == "failed" && r.ExitCode == 0 && (r.Error == "" || strings.ToLower(r.Error) == "success") {
			r.Status = "success"
		}

		if r.Status == "installing" {
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
			if len(appStats[key].topError) > 150 {
				appStats[key].topError = appStats[key].topError[:150] + "..."
			}
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
		case 126:
			desc = "Command cannot execute (permission problem)"
			cat = "permission"
		case 127:
			desc = "Command not found"
			cat = "command_not_found"
		case 128:
			desc = "Invalid argument to exit"
			cat = "unknown"
		case 130:
			desc = "Script terminated by Ctrl+C (SIGINT)"
			cat = "user_aborted"
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
			desc = "Script error (set -e / errexit triggered or SSH error)"
			cat = "unknown"
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
	app             string
	typ             string
	total           int
	failed          int
	aborted         int
	topExitCode     int
	topExitCodeCount int
	topError        string
	topCategory     string
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
			since = time.Now().AddDate(0, 0, -(days - 1)).Format("2006-01-02") + " 00:00:00"
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
	data.TotalAllTime = result.TotalItems    // Actual total in database
	data.SampleSize = len(records)           // How many we actually processed

	// Aggregate statistics
	appCounts := make(map[string]int)
	osCounts := make(map[string]int)
	methodCounts := make(map[string]int)
	pveCounts := make(map[string]int)
	typeCounts := make(map[string]int)
	errorApps := make(map[string]map[string]bool) // pattern -> set of apps
	errorCounts := make(map[string]int)            // pattern -> total occurrences
	dailySuccess := make(map[string]int)
	dailyFailed := make(map[string]int)

	// Failure tracking per app+type
	appTypeCounts := make(map[string]int)
	appTypeFailures := make(map[string]int)

	// Extended metrics maps
	gpuCounts := make(map[string]int)              // "vendor|passthrough" -> count
	errorCatCounts := make(map[string]int)          // category -> count
	toolCounts := make(map[string]int)              // tool_name -> count
	addonCounts := make(map[string]int)             // addon_name -> count
	var totalDuration, durationCount int

	for i := range records {
		r := &records[i]
		data.TotalInstalls++

		// Auto-reclassify: old records still have status="failed" for SIGINT/Ctrl+C
		if r.Status == "failed" && (r.ExitCode == 130 ||
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
		case "installing":
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

		// Track tool executions (type="tool", tool name is in nsapp)
		if r.Type == "tool" && r.NSAPP != "" {
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

// DashboardHTML returns the embedded dashboard HTML
func DashboardHTML() string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analytics - Proxmox VE Helper-Scripts</title>
    <meta name="description" content="Installation analytics and telemetry for Proxmox VE Helper Scripts">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📊</text></svg>">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e14;
            --bg-secondary: #131920;
            --bg-tertiary: #1a2029;
            --bg-card: #151b23;
            --border-color: #2d3748;
            --text-primary: #e2e8f0;
            --text-secondary: #8b949e;
            --text-muted: #64748b;
            --accent-blue: #3b82f6;
            --accent-cyan: #22d3ee;
            --accent-green: #22c55e;
            --accent-red: #ef4444;
            --accent-yellow: #eab308;
            --accent-orange: #f97316;
            --accent-purple: #a855f7;
            --accent-pink: #ec4899;
            --accent-lime: #84cc16;
            --gradient-blue: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
            --gradient-green: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
            --gradient-red: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
            --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
            --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
            --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
        }
        
        [data-theme="light"] {
            --bg-primary: #f8fafc;
            --bg-secondary: #ffffff;
            --bg-tertiary: #f1f5f9;
            --bg-card: #ffffff;
            --border-color: #e2e8f0;
            --text-primary: #1e293b;
            --text-secondary: #64748b;
            --text-muted: #94a3b8;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            line-height: 1.5;
        }
        
        /* Top Navigation Bar */
        .navbar {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            padding: 0 24px;
            height: 64px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(10px);
        }
        
        .navbar-brand {
            display: flex;
            align-items: center;
            gap: 12px;
            text-decoration: none;
            color: var(--text-primary);
            font-weight: 600;
            font-size: 16px;
        }
        
        .navbar-brand svg {
            color: var(--accent-cyan);
        }
        
        .navbar-center {
            flex: 1;
            display: flex;
            justify-content: center;
            padding: 0 40px;
        }
        
        .search-box {
            position: relative;
            width: 100%;
            max-width: 320px;
        }
        
        .search-box input {
            width: 100%;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 10px 16px 10px 40px;
            border-radius: 8px;
            font-size: 14px;
            outline: none;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        
        .search-box input::placeholder {
            color: var(--text-muted);
        }
        
        .search-box input:focus {
            border-color: var(--accent-blue);
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
        }
        
        .search-box svg {
            position: absolute;
            left: 12px;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-muted);
        }
        
        .search-box .shortcut {
            position: absolute;
            right: 12px;
            top: 50%;
            transform: translateY(-50%);
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            color: var(--text-muted);
        }
        
        .navbar-actions {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .github-stars {
            display: flex;
            align-items: center;
            gap: 6px;
            background: var(--accent-yellow);
            color: #000;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
            text-decoration: none;
            transition: transform 0.2s;
        }
        
        .github-stars:hover {
            transform: scale(1.05);
        }
        
        .nav-icon {
            width: 40px;
            height: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 8px;
            color: var(--text-secondary);
            transition: background 0.2s, color 0.2s;
            cursor: pointer;
            border: none;
            background: transparent;
        }
        
        .nav-icon:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        /* Main Content */
        .main-content {
            padding: 24px 40px;
            max-width: 1920px;
            margin: 0 auto;
        }
        
        /* Page Header */
        .page-header {
            margin-bottom: 32px;
        }
        
        .page-header h1 {
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 8px;
        }
        
        .page-header p {
            color: var(--text-secondary);
            font-size: 15px;
        }
        
        /* Stat Cards Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 20px;
            margin-bottom: 32px;
        }
        
        @media (max-width: 1600px) {
            .stats-grid {
                grid-template-columns: repeat(3, 1fr);
            }
        }
        
        @media (max-width: 1200px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        
        @media (max-width: 768px) {
            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
        
        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 24px;
            position: relative;
            overflow: hidden;
        }
        
        .stat-card-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 16px;
        }
        
        .stat-card-label {
            font-size: 14px;
            color: var(--text-secondary);
            font-weight: 500;
        }
        
        .stat-card-icon {
            width: 36px;
            height: 36px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 8px;
            color: var(--text-secondary);
        }
        
        .stat-card-value {
            font-size: 36px;
            font-weight: 700;
            line-height: 1;
            margin-bottom: 6px;
        }
        
        .stat-card-subtitle {
            font-size: 13px;
            color: var(--text-muted);
        }
        
        .stat-card.success .stat-card-icon { color: var(--accent-green); }
        .stat-card.success .stat-card-value { color: var(--accent-green); }
        .stat-card.failed .stat-card-icon { color: var(--accent-red); }
        .stat-card.failed .stat-card-value { color: var(--accent-red); }
        .stat-card.aborted .stat-card-icon { color: var(--accent-purple); }
        .stat-card.aborted .stat-card-value { color: var(--accent-purple); }
        .stat-card.popular .stat-card-icon { color: var(--accent-yellow); }
        .stat-card.popular .stat-card-value { font-size: 24px; }
        
        /* Section Cards */
        .section-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            margin-bottom: 24px;
            overflow: hidden;
        }
        
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 24px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .section-header h2 {
            font-size: 18px;
            font-weight: 600;
        }
        
        .section-header p {
            font-size: 13px;
            color: var(--text-secondary);
            margin-top: 2px;
        }
        
        .section-actions {
            display: flex;
            gap: 8px;
        }
        
        .btn {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 16px;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
            border: 1px solid var(--border-color);
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        .btn:hover {
            background: var(--bg-primary);
            border-color: var(--accent-blue);
        }
        
        .btn-primary {
            background: var(--accent-blue);
            border-color: var(--accent-blue);
            color: #fff;
        }
        
        .btn-primary:hover {
            background: #2563eb;
        }
        
        /* Top Applications Chart */
        .chart-container {
            padding: 24px;
            height: 420px;
        }
        
        /* Filters Section */
        .filters-bar {
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 16px 24px;
            background: var(--bg-tertiary);
            border-bottom: 1px solid var(--border-color);
            flex-wrap: wrap;
        }
        
        .filter-group {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .filter-divider {
            width: 1px;
            height: 32px;
            background: var(--border-color);
            margin: 0 4px;
        }
        
        .filter-group label {
            font-size: 13px;
            color: var(--text-secondary);
            white-space: nowrap;
        }
        
        .quickfilter {
            display: flex;
            gap: 4px;
            background: var(--bg-secondary);
            padding: 4px;
            border-radius: 8px;
            border: 1px solid var(--border-color);
        }
        
        .filter-btn {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            padding: 6px 14px;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .filter-btn:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        .filter-btn.active {
            background: var(--accent-blue);
            color: #fff;
        }

        .source-btn {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            padding: 6px 14px;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .source-btn:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        .source-btn.active {
            background: var(--accent-green, #22c55e);
            color: #fff;
        }
        
        /* Custom Select */
        .custom-select {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 8px 32px 8px 12px;
            border-radius: 8px;
            font-size: 13px;
            cursor: pointer;
            outline: none;
            appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%238b949e' stroke-width='2'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 10px center;
        }
        
        .custom-select:focus {
            border-color: var(--accent-blue);
        }
        
        .search-input {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 8px 12px;
            border-radius: 8px;
            font-size: 13px;
            outline: none;
            min-width: 200px;
        }
        
        .search-input:focus {
            border-color: var(--accent-blue);
        }
        
        .search-input::placeholder {
            color: var(--text-muted);
        }
        
        /* Table Styles */
        .table-wrapper {
            overflow-x: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
        }
        
        th, td {
            padding: 14px 20px;
            text-align: left;
        }
        
        th {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            background: var(--bg-tertiary);
            border-bottom: 1px solid var(--border-color);
            white-space: nowrap;
        }
        
        th.sortable {
            cursor: pointer;
            user-select: none;
            transition: color 0.2s;
        }
        
        th.sortable:hover {
            color: var(--accent-blue);
        }
        
        th.sort-asc, th.sort-desc {
            color: var(--accent-blue);
        }
        
        td {
            font-size: 14px;
            border-bottom: 1px solid var(--border-color);
        }
        
        tr:hover td {
            background: rgba(59, 130, 246, 0.05);
        }
        
        tr.clickable-row {
            cursor: pointer;
        }
        
        /* Status Badge */
        .status-badge {
            display: inline-flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 600;
            text-transform: capitalize;
            border: 1px solid transparent;
        }
        
        .status-badge.success {
            background: rgba(34, 197, 94, 0.15);
            color: var(--accent-green);
            border-color: rgba(34, 197, 94, 0.3);
        }
        
        .status-badge.failed {
            background: rgba(239, 68, 68, 0.15);
            color: var(--accent-red);
            border-color: rgba(239, 68, 68, 0.3);
        }
        
        .status-badge.installing {
            background: rgba(234, 179, 8, 0.15);
            color: var(--accent-yellow);
            border-color: rgba(234, 179, 8, 0.3);
        }
        
        .status-badge.aborted {
            background: rgba(168, 85, 247, 0.15);
            color: var(--accent-purple);
            border-color: rgba(168, 85, 247, 0.3);
        }
        
        .status-badge.unknown {
            background: rgba(100, 116, 139, 0.15);
            color: var(--text-muted);
            border-color: rgba(100, 116, 139, 0.3);
        }
        
        /* Type Badge */
        .type-badge {
            display: inline-flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .type-badge.lxc {
            background: rgba(34, 211, 238, 0.15);
            color: var(--accent-cyan);
            border: 1px solid rgba(34, 211, 238, 0.3);
        }
        
        .type-badge.vm {
            background: rgba(168, 85, 247, 0.15);
            color: var(--accent-purple);
            border: 1px solid rgba(168, 85, 247, 0.3);
        }
        
        .type-badge.tool {
            background: rgba(249, 115, 22, 0.15);
            color: var(--accent-orange);
            border: 1px solid rgba(249, 115, 22, 0.3);
        }
        
        .type-badge.addon {
            background: rgba(236, 72, 153, 0.15);
            color: var(--accent-pink);
            border: 1px solid rgba(236, 72, 153, 0.3);
        }
        
        /* Pagination */
        .table-footer {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 24px;
            background: var(--bg-tertiary);
            border-top: 1px solid var(--border-color);
        }
        
        .pagination {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .pagination button {
            padding: 8px 14px;
            border-radius: 6px;
            font-size: 13px;
            border: 1px solid var(--border-color);
            background: var(--bg-secondary);
            color: var(--text-primary);
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .pagination button:hover:not(:disabled) {
            border-color: var(--accent-blue);
            background: var(--bg-primary);
        }
        
        .pagination button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .pagination-info {
            font-size: 13px;
            color: var(--text-secondary);
        }
        
        .per-page-select {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .per-page-select label {
            font-size: 13px;
            color: var(--text-secondary);
        }
        
        /* Loading & Error States */
        .loading {
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: 60px 20px;
            color: var(--text-secondary);
        }
        
        .loading-spinner {
            width: 40px;
            height: 40px;
            border: 3px solid var(--border-color);
            border-top-color: var(--accent-blue);
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-bottom: 16px;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        .error-banner {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            color: var(--accent-red);
            padding: 16px 24px;
            border-radius: 8px;
            margin-bottom: 24px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        /* Modal Styles */
        .modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.75);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
            opacity: 0;
            visibility: hidden;
            transition: opacity 0.2s, visibility 0.2s;
            backdrop-filter: blur(4px);
        }
        
        .modal-overlay.active {
            opacity: 1;
            visibility: visible;
        }
        
        .modal-content {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            width: 90%;
            max-width: 700px;
            max-height: 90vh;
            overflow-y: auto;
            transform: scale(0.95) translateY(10px);
            transition: transform 0.2s;
            box-shadow: var(--shadow-lg);
        }
        
        .modal-overlay.active .modal-content {
            transform: scale(1) translateY(0);
        }
        
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 24px;
            border-bottom: 1px solid var(--border-color);
            position: sticky;
            top: 0;
            background: var(--bg-card);
            z-index: 10;
        }
        
        .modal-header h2 {
            font-size: 18px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .modal-close {
            width: 36px;
            height: 36px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 8px;
            background: transparent;
            border: none;
            color: var(--text-secondary);
            font-size: 20px;
            cursor: pointer;
            transition: background 0.2s;
        }
        
        .modal-close:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        .modal-body {
            padding: 24px;
        }
        
        /* Detail Modal Sections */
        .detail-section {
            margin-bottom: 24px;
        }
        
        .detail-section:last-child {
            margin-bottom: 0;
        }
        
        .detail-section-header {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .detail-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
        }
        
        .detail-item {
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 12px 16px;
        }
        
        .detail-item .label {
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.3px;
            margin-bottom: 4px;
        }
        
        .detail-item .value {
            font-size: 14px;
            font-weight: 500;
            word-break: break-word;
        }
        
        .detail-item .value.mono {
            font-family: 'SF Mono', 'Consolas', monospace;
            font-size: 12px;
        }
        
        .detail-item .value.status-success { color: var(--accent-green); }
        .detail-item .value.status-failed { color: var(--accent-red); }
        .detail-item .value.status-installing { color: var(--accent-yellow); }
        
        .error-box {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            border-radius: 8px;
            padding: 16px;
            font-family: 'SF Mono', 'Consolas', monospace;
            font-size: 12px;
            color: var(--accent-red);
            white-space: pre-wrap;
            word-break: break-word;
            max-height: 400px;
            overflow-y: auto;
        }
        
        /* Health Modal */
        .health-modal {
            max-width: 420px;
        }
        
        .health-status {
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 16px;
        }
        
        .health-status.ok {
            background: rgba(34, 197, 94, 0.1);
            border: 1px solid rgba(34, 197, 94, 0.3);
        }
        
        .health-status.error {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
        }
        
        .health-status .icon {
            font-size: 36px;
        }
        
        .health-status .details .title {
            font-weight: 600;
            font-size: 16px;
        }
        
        .health-status .details .subtitle {
            font-size: 13px;
            color: var(--text-secondary);
            margin-top: 4px;
        }
        
        .health-info {
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 16px;
        }
        
        .health-info div {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            font-size: 13px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .health-info div:last-child {
            border-bottom: none;
        }
        
        /* Secondary Charts Section */
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            margin-bottom: 24px;
        }
        
        @media (max-width: 1200px) {
            .charts-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        
        @media (max-width: 768px) {
            .charts-grid {
                grid-template-columns: 1fr;
            }
        }
        
        .chart-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 20px;
        }
        
        .chart-card h3 {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 16px;
            color: var(--text-secondary);
        }
        
        .chart-card .chart-wrapper {
            height: 200px;
            font-size: 12px;
            font-weight: 600;
        }
        
        /* Error Analysis Combined Section */
        .error-analysis-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 24px;
            padding: 0 24px 24px;
        }
        @media (max-width: 900px) {
            .error-analysis-grid { grid-template-columns: 1fr; }
        }
        .error-analysis-col {
            min-width: 0;
        }
        .error-analysis-subtitle {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
            margin: 0 0 12px 0;
            padding: 0;
        }
        
        .failed-apps-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 8px;
        }
        .failed-app-card {
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 10px 14px;
            transition: background 0.15s;
        }
        .failed-app-card:hover {
            background: var(--bg-secondary);
        }
        .failed-app-card .app-info {
            display: flex;
            align-items: center;
            gap: 8px;
            min-width: 0;
        }
        .failed-app-card .app-name {
            font-weight: 600;
            font-size: 13px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .failed-app-card .details {
            font-size: 11px;
            color: var(--text-secondary);
            white-space: nowrap;
        }
        .failed-app-card .failure-rate {
            font-size: 15px;
            font-weight: 700;
            white-space: nowrap;
            padding-left: 12px;
        }
        .failed-app-card .failure-rate.critical { color: var(--accent-red); }
        .failed-app-card .failure-rate.warning { color: var(--accent-orange); }
        .failed-app-card .failure-rate.moderate { color: var(--accent-yellow, #eab308); }
        .failed-app-card .type-badge {
            font-size: 10px;
            padding: 2px 6px;
            vertical-align: middle;
            flex-shrink: 0;
        }
        
        /* Error Analysis List */
        .error-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .error-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 14px;
            background: var(--bg-tertiary);
            border-radius: 8px;
            transition: background 0.15s;
        }
        .error-item:hover {
            background: var(--bg-secondary);
        }
        .error-item .pattern {
            font-weight: 600;
            font-size: 13px;
            color: var(--accent-red);
            margin-bottom: 2px;
        }
        .error-item .meta {
            font-size: 11px;
            color: var(--text-secondary);
        }
        .count-badge {
            background: rgba(239, 68, 68, 0.12);
            color: var(--accent-red);
            padding: 4px 10px;
            border-radius: 16px;
            font-size: 12px;
            font-weight: 600;
            white-space: nowrap;
        }
        
        /* Mini Podium in Stat Card */
        .podium-card {
            min-width: 280px;
        }
        
        .mini-podium {
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-top: 8px;
        }
        
        .mini-podium-item {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 8px 12px;
            border-radius: 8px;
            background: var(--bg-body);
        }
        
        .mini-podium-item .medal {
            font-size: 20px;
            flex-shrink: 0;
        }
        
        .mini-podium-item .app-name {
            flex: 1;
            font-weight: 600;
            font-size: 14px;
            color: var(--text-primary);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        
        .mini-podium-item .count {
            font-size: 12px;
            color: var(--text-secondary);
            flex-shrink: 0;
        }
        
        .mini-podium-item.gold {
            background: linear-gradient(135deg, rgba(255, 215, 0, 0.15) 0%, rgba(255, 215, 0, 0.05) 100%);
            border-left: 3px solid #ffd700;
        }
        
        .mini-podium-item.silver {
            background: linear-gradient(135deg, rgba(192, 192, 192, 0.15) 0%, rgba(192, 192, 192, 0.05) 100%);
            border-left: 3px solid #c0c0c0;
        }
        
        .mini-podium-item.bronze {
            background: linear-gradient(135deg, rgba(205, 127, 50, 0.15) 0%, rgba(205, 127, 50, 0.05) 100%);
            border-left: 3px solid #cd7f32;
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
        
        /* Auto-Refresh Toggle */
        .auto-refresh-toggle {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
            color: var(--text-secondary);
        }
        
        .toggle-switch {
            position: relative;
            display: inline-block;
            width: 44px;
            height: 24px;
        }
        
        .toggle-switch input {
            opacity: 0;
            width: 0;
            height: 0;
        }
        
        .toggle-slider {
            position: absolute;
            cursor: pointer;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-color: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            transition: .3s;
            border-radius: 24px;
        }
        
        .toggle-slider:before {
            position: absolute;
            content: "";
            height: 16px;
            width: 16px;
            left: 3px;
            bottom: 3px;
            background-color: var(--text-muted);
            transition: .3s;
            border-radius: 50%;
        }
        
        .toggle-switch input:checked + .toggle-slider {
            background-color: rgba(34, 197, 94, 0.2);
            border-color: var(--accent-green);
        }
        
        .toggle-switch input:checked + .toggle-slider:before {
            transform: translateX(20px);
            background-color: var(--accent-green);
        }
        
        .auto-refresh-interval {
            font-size: 11px;
            color: var(--text-muted);
            font-family: monospace;
        }
        
        .auto-refresh-interval.active {
            color: var(--accent-green);
        }
        
        /* Detail Modal Styles */
        .modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.7);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
            opacity: 0;
            visibility: hidden;
            transition: opacity 0.2s, visibility 0.2s;
        }
        
        .modal-overlay.active {
            opacity: 1;
            visibility: visible;
        }
        
        .modal-content {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            width: 90%;
            max-width: 700px;
            max-height: 90vh;
            overflow-y: auto;
            transform: scale(0.9);
            transition: transform 0.2s;
        }
        
        .modal-overlay.active .modal-content {
            transform: scale(1);
        }
        
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 24px;
            border-bottom: 1px solid var(--border-color);
            position: sticky;
            top: 0;
            background: var(--bg-secondary);
            z-index: 10;
        }
        
        .modal-header h2 {
            font-size: 20px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .modal-close {
            background: none;
            border: none;
            color: var(--text-secondary);
            font-size: 24px;
            cursor: pointer;
            padding: 4px 8px;
            border-radius: 4px;
        }
        
        .modal-close:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        .modal-body {
            padding: 24px;
        }
        
        .detail-section {
            margin-bottom: 24px;
        }
        
        .detail-section:last-child {
            margin-bottom: 0;
        }
        
        .detail-section-header {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border-color);
        }
        
        .detail-section-header svg {
            opacity: 0.7;
        }
        
        .detail-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
        }
        
        .detail-item {
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 12px 16px;
        }
        
        .detail-item .label {
            font-size: 11px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.3px;
            margin-bottom: 4px;
        }
        
        .detail-item .value {
            font-size: 15px;
            font-weight: 500;
            word-break: break-word;
        }
        
        .detail-item .value.mono {
            font-family: 'SF Mono', 'Consolas', monospace;
            font-size: 13px;
        }
        
        .detail-item.full-width {
            grid-column: 1 / -1;
        }
        
        .detail-item .value.status-success { color: var(--accent-green); }
        .detail-item .value.status-failed { color: var(--accent-red); }
        .detail-item .value.status-installing { color: var(--accent-yellow); }
        
        .error-box {
            background: rgba(248, 81, 73, 0.1);
            border: 1px solid rgba(248, 81, 73, 0.3);
            border-radius: 8px;
            padding: 16px;
            font-family: 'SF Mono', 'Consolas', monospace;
            font-size: 13px;
            color: var(--accent-red);
            white-space: pre-wrap;
            word-break: break-word;
            max-height: 400px;
            overflow-y: auto;
        }
        
        tr.clickable-row {
            cursor: pointer;
            transition: background 0.15s;
        }
        
        tr.clickable-row:hover {
            background: rgba(88, 166, 255, 0.1) !important;
        }
    </style>
</head>
<body>
    <!-- Navigation Bar -->
    <nav class="navbar">
        <a href="https://community-scripts.github.io/ProxmoxVE/" class="navbar-brand" target="_blank">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                <polyline points="4 17 10 11 4 5"/>
                <line x1="12" y1="19" x2="20" y2="19"/>
            </svg>
            Proxmox VE Helper-Scripts
        </a>
        
        <div class="navbar-center">
            <div style="display: flex; align-items: center; gap: 12px;">
                <span id="loadingIndicator" style="display: none; color: var(--accent-cyan); font-size: 14px;">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="animation: spin 1s linear infinite; margin-right: 6px;">
                        <circle cx="12" cy="12" r="10" stroke-opacity="0.3"/>
                        <path d="M12 2a10 10 0 0 1 10 10"/>
                    </svg>
                    Loading data...
                </span>
                <span id="cacheStatus" style="font-size: 12px; color: var(--text-muted);"></span>
            </div>
        </div>
        
        <div class="navbar-actions">
            <a href="https://github.com/community-scripts/ProxmoxVE" target="_blank" class="github-stars" id="githubStars">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
                </svg>
                <span id="starCount">-</span>
            </a>
            <a href="https://github.com/community-scripts/ProxmoxVE" target="_blank" class="nav-icon" title="GitHub">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                </svg>
            </a>
            <a href="https://discord.gg/2wvnMDgeFz" target="_blank" class="nav-icon" title="Discord">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M20.317 4.37a19.79 19.79 0 00-4.885-1.515.074.074 0 00-.079.037c-.21.375-.444.865-.608 1.25a18.27 18.27 0 00-5.487 0 12.64 12.64 0 00-.617-1.25.077.077 0 00-.079-.037A19.74 19.74 0 003.677 4.37a.07.07 0 00-.032.028C.533 9.046-.32 13.58.099 18.057a.082.082 0 00.031.057 19.9 19.9 0 005.993 3.03.078.078 0 00.084-.028c.462-.63.873-1.295 1.226-1.994a.076.076 0 00-.041-.106 13.11 13.11 0 01-1.872-.892.077.077 0 01-.008-.128c.126-.094.252-.192.372-.291a.074.074 0 01.078-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 01.078.009c.12.1.246.198.373.292a.077.077 0 01-.006.127 12.3 12.3 0 01-1.873.892.076.076 0 00-.041.107c.36.698.772 1.363 1.225 1.993a.076.076 0 00.084.029 19.84 19.84 0 006.002-3.03.077.077 0 00.032-.054c.5-5.177-.838-9.674-3.549-13.66a.06.06 0 00-.031-.03zM8.02 15.33c-1.183 0-2.157-1.086-2.157-2.419s.955-2.419 2.157-2.419c1.21 0 2.176 1.096 2.157 2.42 0 1.332-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.086-2.157-2.419s.955-2.419 2.157-2.419c1.21 0 2.176 1.096 2.157 2.42 0 1.332-.946 2.418-2.157 2.418z"/>
                </svg>
            </a>
            <button class="theme-toggle" onclick="toggleTheme()" title="Toggle theme">
                <span id="themeIcon">🌙</span>
            </button>
        </div>
    </nav>
    
    <!-- Main Content -->
    <div class="main-content">
        <!-- Page Header -->
        <div class="page-header">
            <h1>Analytics</h1>
            <p>Overview of container installations and system statistics.</p>
        </div>
        
        <!-- Filters Bar -->
        <div class="filters-bar" style="background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; margin-bottom: 24px;">
            <div class="filter-group">
                <label>Source:</label>
                <div class="quickfilter">
                    <button class="source-btn active" data-repo="ProxmoxVE">ProxmoxVE</button>
                    <button class="source-btn" data-repo="ProxmoxVED">ProxmoxVED</button>
                    <button class="source-btn" data-repo="external">External</button>
                    <button class="source-btn" data-repo="all">All</button>
                </div>
            </div>
            <div class="filter-divider"></div>
            <div class="filter-group">
                <label>Period:</label>
                <div class="quickfilter">
                    <button class="filter-btn active" data-days="1">Today</button>
                    <button class="filter-btn" data-days="7">7 Days</button>
                    <button class="filter-btn" data-days="30">30 Days</button>
                    <button class="filter-btn" data-days="90">90 Days</button>
                    <button class="filter-btn" data-days="365">1 Year</button>
                </div>
            </div>
            <div class="filter-divider"></div>
            <div class="auto-refresh-toggle">
                <label class="toggle-switch">
                    <input type="checkbox" id="autoRefreshToggle" onchange="toggleAutoRefresh()">
                    <span class="toggle-slider"></span>
                </label>
                <span>Auto-refresh</span>
                <span class="auto-refresh-interval" id="refreshInterval">15s</span>
            </div>
            <div style="margin-left: auto; display: flex; gap: 8px;">
                <button class="btn" onclick="refreshData()">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M23 4v6h-6"/><path d="M1 20v-6h6"/>
                        <path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/>
                    </svg>
                    Refresh
                </button>
                <span class="last-updated" id="lastUpdated" style="align-self: center; font-size: 12px; color: var(--text-muted);"></span>
            </div>
        </div>
        
        <!-- Error Banner -->
        <div id="error" class="error-banner" style="display: none;">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"/>
                <line x1="12" y1="8" x2="12" y2="12"/>
                <line x1="12" y1="16" x2="12.01" y2="16"/>
            </svg>
            <span id="errorText"></span>
        </div>
        
        <!-- Stats Cards -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-card-header">
                    <span class="stat-card-label">Total Created</span>
                    <div class="stat-card-icon">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                            <line x1="9" y1="9" x2="15" y2="9"/>
                            <line x1="9" y1="13" x2="15" y2="13"/>
                            <line x1="9" y1="17" x2="11" y2="17"/>
                        </svg>
                    </div>
                </div>
                <div class="stat-card-value" id="totalInstalls">-</div>
                <div class="stat-card-subtitle">Total LXC/VM entries found</div>
            </div>
            
            <div class="stat-card success">
                <div class="stat-card-header">
                    <span class="stat-card-label">Success Rate</span>
                    <div class="stat-card-icon">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                            <polyline points="22 4 12 14.01 9 11.01"/>
                        </svg>
                    </div>
                </div>
                <div class="stat-card-value" id="successRate">-</div>
                <div class="stat-card-subtitle" id="successSubtitle">successful installations</div>
            </div>
            
            <div class="stat-card failed">
                <div class="stat-card-header">
                    <span class="stat-card-label">Failed</span>
                    <div class="stat-card-icon">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"/>
                            <line x1="15" y1="9" x2="9" y2="15"/>
                            <line x1="9" y1="9" x2="15" y2="15"/>
                        </svg>
                    </div>
                </div>
                <div class="stat-card-value" id="failedCount">-</div>
                <div class="stat-card-subtitle" id="failedSubtitle">installation failures</div>
            </div>
            
            <div class="stat-card aborted">
                <div class="stat-card-header">
                    <span class="stat-card-label">Aborted</span>
                    <div class="stat-card-icon">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"/>
                            <line x1="12" y1="8" x2="12" y2="12"/>
                            <line x1="12" y1="16" x2="12.01" y2="16"/>
                        </svg>
                    </div>
                </div>
                <div class="stat-card-value" id="abortedCount">-</div>
                <div class="stat-card-subtitle">user cancelled (Ctrl+C)</div>
            </div>
            
            <!-- Most Popular Card -->
            <div class="stat-card podium-card">
                <div class="stat-card-header">
                    <span class="stat-card-label">Most Popular</span>
                    <div class="stat-card-icon" style="font-size: 20px;">🏆</div>
                </div>
                <div class="mini-podium">
                    <div class="mini-podium-item gold">
                        <span class="medal">🥇</span>
                        <span class="app-name" id="podium1App">-</span>
                        <span class="count" id="podium1Count">-</span>
                    </div>
                    <div class="mini-podium-item silver">
                        <span class="medal">🥈</span>
                        <span class="app-name" id="podium2App">-</span>
                        <span class="count" id="podium2Count">-</span>
                    </div>
                    <div class="mini-podium-item bronze">
                        <span class="medal">🥉</span>
                        <span class="app-name" id="podium3App">-</span>
                        <span class="count" id="podium3Count">-</span>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Top Applications Section -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Top Applications</h2>
                    <p>The most frequently installed applications.</p>
                </div>
                <div class="section-actions">
                    <button class="btn" id="viewAllAppsBtn" onclick="toggleAllApps()">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="8" y1="6" x2="21" y2="6"/>
                            <line x1="8" y1="12" x2="21" y2="12"/>
                            <line x1="8" y1="18" x2="21" y2="18"/>
                            <line x1="3" y1="6" x2="3.01" y2="6"/>
                            <line x1="3" y1="12" x2="3.01" y2="12"/>
                            <line x1="3" y1="18" x2="3.01" y2="18"/>
                        </svg>
                        View All
                    </button>
                </div>
            </div>
            <div class="chart-container" id="appsChartContainer">
                <canvas id="appsChart"></canvas>
            </div>
        </div>
        
        <!-- Secondary Charts -->
        <div class="section-card" style="margin-bottom: 24px;">
            <div class="section-header">
                <div>
                    <h2>Installations Over Time</h2>
                    <p>Daily success and failure trends.</p>
                </div>
            </div>
            <div style="padding: 24px; height: 320px;">
                <canvas id="dailyChart"></canvas>
            </div>
        </div>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px;">
            <div class="chart-card">
                <h3>OS Distribution</h3>
                <div class="chart-wrapper" style="height: 260px;">
                    <canvas id="osChart"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h3>Status Distribution</h3>
                <div class="chart-wrapper">
                    <canvas id="statusChart"></canvas>
                </div>
            </div>
        </div>
        
        <!-- Error Analysis Section (combined) -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Error Analysis</h2>
                    <p>Error patterns, failure rates, and applications that need attention. <span id="failedAppsThreshold" style="color: var(--text-muted); font-size: 12px;"></span></p>
                </div>
                <div class="section-actions">
                    <a href="/error-analysis" class="btn" style="text-decoration:none;">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="11" cy="11" r="8"/>
                            <line x1="21" y1="21" x2="16.65" y2="16.65"/>
                        </svg>
                        Deep Analysis
                    </a>
                </div>
            </div>
            <div class="error-analysis-grid">
                <div class="error-analysis-col">
                    <h3 class="error-analysis-subtitle">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--accent-red)" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
                        Common Error Patterns
                    </h3>
                    <div class="error-list" id="errorList">
                        <div class="loading"><div class="loading-spinner"></div>Loading...</div>
                    </div>
                </div>
                <div class="error-analysis-col">
                    <h3 class="error-analysis-subtitle">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--accent-orange)" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
                        Highest Failure Rates
                    </h3>
                    <div class="failed-apps-grid" id="failedAppsGrid">
                        <div class="loading"><div class="loading-spinner"></div>Loading...</div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Installation Log Section -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Installation Log</h2>
                    <p>Detailed records of all container creation attempts.</p>
                </div>
                <div class="section-actions">
                    <button class="btn" onclick="exportCSV()">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                            <polyline points="7 10 12 15 17 10"/>
                            <line x1="12" y1="15" x2="12" y2="3"/>
                        </svg>
                        Export CSV
                    </button>
                </div>
            </div>
            <div class="filters-bar">
                <input type="text" class="search-input" id="filterApp" placeholder="Filter by application..." oninput="filterTable()">
                <select id="filterStatus" class="custom-select" onchange="filterTable()">
                    <option value="">All Status</option>
                    <option value="success">Success</option>
                    <option value="failed">Failed</option>
                    <option value="aborted">Aborted</option>
                    <option value="installing">Installing</option>
                    <option value="unknown">Unknown</option>
                </select>
                <select id="filterOs" class="custom-select" onchange="filterTable()">
                    <option value="">All OS</option>
                </select>
                <select id="filterType" class="custom-select" onchange="filterTable()">
                    <option value="">All Types</option>
                    <option value="lxc">LXC</option>
                    <option value="vm">VM</option>
                </select>
            </div>
            <div class="table-wrapper">
                <table id="installTable">
                    <thead>
                        <tr>
                            <th data-sort="status" class="sortable">Status</th>
                            <th data-sort="type" class="sortable">Type</th>
                            <th data-sort="nsapp" class="sortable">Application</th>
                            <th data-sort="os_type" class="sortable">OS</th>
                            <th>Disk Size</th>
                            <th>Core Count</th>
                            <th>RAM Size</th>
                            <th data-sort="created" class="sortable sort-desc">Created At</th>
                        </tr>
                    </thead>
                    <tbody id="recordsTable">
                        <tr><td colspan="8"><div class="loading"><div class="loading-spinner"></div>Loading...</div></td></tr>
                    </tbody>
                </table>
            </div>
            <div class="table-footer">
                <div class="per-page-select">
                    <label>Show:</label>
                    <select id="perPageSelect" class="custom-select" onchange="changePerPage()">
                        <option value="25" selected>25</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                    </select>
                </div>
                <div class="pagination">
                    <button onclick="prevPage()" id="prevBtn" disabled>Previous</button>
                    <span class="pagination-info" id="pageInfo">Page 1</span>
                    <button onclick="nextPage()" id="nextBtn">Next</button>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Health Check Modal -->
    <div class="modal-overlay" id="healthModal" onclick="closeHealthModal(event)">
        <div class="modal-content health-modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h2>Health Check</h2>
                <button class="modal-close" onclick="closeHealthModal()">&times;</button>
            </div>
            <div class="modal-body" id="healthModalBody">
                <div class="loading"><div class="loading-spinner"></div>Checking...</div>
            </div>
        </div>
    </div>
    
    <!-- Detail Modal -->
    <div class="modal-overlay" id="detailModal" onclick="closeModalOutside(event)">
        <div class="modal-content" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h2 id="modalTitle">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                        <line x1="9" y1="9" x2="15" y2="9"/>
                        <line x1="9" y1="13" x2="15" y2="13"/>
                        <line x1="9" y1="17" x2="11" y2="17"/>
                    </svg>
                    <span>Record Details</span>
                </h2>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body" id="modalBody">
                <!-- Content filled by JavaScript -->
            </div>
        </div>
    </div>
    
    <script>
        let charts = {};
        let allRecords = [];
        let allAppsData = [];
        let showingAllApps = false;
        let currentPage = 1;
        let totalPages = 1;
        let perPage = 25;
        let currentTheme = localStorage.getItem('theme') || 'dark';
        let currentSort = { field: 'created', dir: 'desc' };
        
        // Auto-refresh state
        let autoRefreshEnabled = localStorage.getItem('autoRefresh') === 'true';
        let autoRefreshInterval = 15000; // 15 seconds
        let autoRefreshTimer = null;
        
        // Colorful palette for Top Applications chart
        const appBarColors = [
            '#3b82f6', '#f97316', '#22c55e', '#a855f7', '#ef4444',
            '#22d3ee', '#eab308', '#ec4899', '#84cc16', '#6366f1',
            '#14b8a6', '#f43f5e', '#8b5cf6', '#10b981', '#06b6d4',
            '#d946ef', '#facc15', '#2dd4bf'
        ];
        
        // Apply saved theme on load
        if (currentTheme === 'light') {
            document.documentElement.setAttribute('data-theme', 'light');
            document.getElementById('themeIcon').textContent = '☀️';
        }
        
        // Fetch GitHub stars
        async function fetchGitHubStars() {
            try {
                const resp = await fetch('https://api.github.com/repos/community-scripts/ProxmoxVE');
                const data = await resp.json();
                if (data.stargazers_count) {
                    document.getElementById('starCount').textContent = data.stargazers_count.toLocaleString();
                }
            } catch (e) {
                console.log('Could not fetch GitHub stars');
            }
        }
        fetchGitHubStars();
        
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
            if (Object.keys(charts).length > 0) {
                refreshData();
            }
        }
        
        function handleGlobalSearch(event) {
            if (event.key === 'Enter') {
                const query = event.target.value.trim();
                if (query) {
                    document.getElementById('filterApp').value = query;
                    filterTable();
                    document.querySelector('.section-card:last-of-type').scrollIntoView({ behavior: 'smooth' });
                }
            }
        }
        
        // Keyboard shortcut for search
        document.addEventListener('keydown', function(e) {
            if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
                e.preventDefault();
                document.getElementById('globalSearch').focus();
            }
        });
        
        const chartDefaults = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: { color: '#8b949e' }
                }
            },
            scales: {
                x: {
                    ticks: { color: '#8b949e' },
                    grid: { color: '#2d3748' }
                },
                y: {
                    ticks: { color: '#8b949e' },
                    grid: { color: '#2d3748' }
                }
            }
        };
        
        async function fetchData() {
            const activeBtn = document.querySelector('.filter-btn.active');
            const days = activeBtn ? activeBtn.dataset.days : '1';
            const repo = document.querySelector('.source-btn.active')?.dataset.repo || 'ProxmoxVE';
            
            // Show loading indicator
            document.getElementById('loadingIndicator').style.display = 'flex';
            document.getElementById('cacheStatus').textContent = '';
            
            try {
                // Add cache-busting timestamp for filter changes to ensure fresh data
                const cacheBuster = '&_t=' + Date.now();
                const response = await fetch('/api/dashboard?days=' + days + '&repo=' + repo + cacheBuster);
                if (!response.ok) throw new Error('Failed to fetch data');
                
                // Check cache status from header
                const cacheHit = response.headers.get('X-Cache') === 'HIT';
                document.getElementById('cacheStatus').textContent = cacheHit ? '(cached)' : '(fresh)';
                
                return await response.json();
            } catch (error) {
                document.getElementById('error').style.display = 'flex';
                document.getElementById('errorText').textContent = error.message;
                throw error;
            } finally {
                document.getElementById('loadingIndicator').style.display = 'none';
            }
        }
        
        function updateStats(data) {
            // Use total_all_time for display if available, otherwise total_installs
            const displayTotal = data.total_all_time || data.total_installs;
            document.getElementById('totalInstalls').textContent = displayTotal.toLocaleString();
            
            // Failed count (separate card)
            document.getElementById('failedCount').textContent = (data.failed_count || 0).toLocaleString();
            document.getElementById('failedSubtitle').textContent = data.failed_count > 0 ? 'installation failures' : 'no failures';
            
            // Aborted count (separate card)
            document.getElementById('abortedCount').textContent = (data.aborted_count || 0).toLocaleString();
            
            document.getElementById('successRate').textContent = data.success_rate.toFixed(1) + '%';
            document.getElementById('successSubtitle').textContent = data.success_count.toLocaleString() + ' successful installations';
            document.getElementById('lastUpdated').textContent = 'Updated ' + new Date().toLocaleTimeString();
            document.getElementById('error').style.display = 'none';
            
            // Most Popular - update podium
            function formatCompact(n) {
                if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
                if (n >= 1000) return (n/1000).toFixed(1) + 'k';
                return n.toString();
            }
            if (data.top_apps && data.top_apps.length >= 3) {
                document.getElementById('podium1App').textContent = data.top_apps[0].app;
                document.getElementById('podium1Count').textContent = formatCompact(data.top_apps[0].count);
                document.getElementById('podium2App').textContent = data.top_apps[1].app;
                document.getElementById('podium2Count').textContent = formatCompact(data.top_apps[1].count);
                document.getElementById('podium3App').textContent = data.top_apps[2].app;
                document.getElementById('podium3Count').textContent = formatCompact(data.top_apps[2].count);
            } else if (data.top_apps && data.top_apps.length > 0) {
                document.getElementById('podium1App').textContent = data.top_apps[0].app;
                document.getElementById('podium1Count').textContent = formatCompact(data.top_apps[0].count);
            }
            
            // Store all apps data for View All feature
            allAppsData = data.top_apps || [];
            
            // Error Analysis
            updateErrorAnalysis(data.error_analysis || []);
            
            // Failed Apps
            updateFailedApps(data.failed_apps || []);
        }
        
        function updateErrorAnalysis(errors) {
            const container = document.getElementById('errorList');
            if (!errors || errors.length === 0) {
                container.innerHTML = '<div style="padding: 20px; color: var(--text-muted); text-align: center; font-size: 13px;">No errors recorded</div>';
                return;
            }
            container.innerHTML = errors.slice(0, 6).map(e => 
                '<div class="error-item">' +
                    '<div style="min-width:0;flex:1;">' +
                        '<div class="pattern">' + escapeHtml(e.pattern) + '</div>' +
                        '<div class="meta">' + e.unique_apps + ' app' + (e.unique_apps !== 1 ? 's' : '') + ' affected</div>' +
                    '</div>' +
                    '<span class="count-badge">' + e.count.toLocaleString() + 'x</span>' +
                '</div>'
            ).join('');
        }
        
        function updateFailedApps(apps) {
            const container = document.getElementById('failedAppsGrid');
            const activeDays = parseInt(document.querySelector('.filter-btn.active')?.dataset.days || '1');
            let minInstalls = 10;
            if (activeDays <= 1) minInstalls = 5;
            else if (activeDays <= 7) minInstalls = 15;
            else if (activeDays <= 30) minInstalls = 40;
            else if (activeDays <= 90) minInstalls = 100;
            else minInstalls = 100;
            document.getElementById('failedAppsThreshold').textContent = '(min. ' + minInstalls + ' installs)';
            if (!apps || apps.length === 0) {
                container.innerHTML = '<div style="padding: 20px; color: var(--text-muted); text-align: center; font-size: 13px;">Not enough data (min. ' + minInstalls + ' installs)</div>';
                return;
            }
            container.innerHTML = apps.slice(0, 8).map(a => {
                const typeClass = (a.type || '').toLowerCase();
                const typeBadge = a.type && a.type !== 'unknown' ? '<span class="type-badge ' + typeClass + '">' + a.type.toUpperCase() + '</span>' : '';
                const rate = a.failure_rate;
                const severityClass = rate >= 30 ? 'critical' : rate >= 15 ? 'warning' : 'moderate';
                return '<div class="failed-app-card">' +
                    '<div class="app-info">' + typeBadge + '<span class="app-name">' + escapeHtml(a.app) + '</span>' +
                        '<span class="details">' + a.failed_count + '/' + a.total_count + '</span>' +
                    '</div>' +
                    '<span class="failure-rate ' + severityClass + '">' + rate.toFixed(1) + '%</span>' +
                '</div>';
            }).join('');
        }
        
        function escapeHtml(str) {
            if (!str) return '';
            return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }
        
        function formatTimestamp(ts) {
            if (!ts) return '-';
            const d = new Date(ts);
            // Format: "Feb 11, 2026, 4:33 PM"
            return d.toLocaleDateString('en-US', { 
                month: 'short', 
                day: 'numeric', 
                year: 'numeric',
                hour: 'numeric',
                minute: '2-digit',
                hour12: true
            });
        }
        
        function initSortableHeaders() {
            document.querySelectorAll('th.sortable').forEach(th => {
                th.style.cursor = 'pointer';
                th.addEventListener('click', () => sortByColumn(th.dataset.sort));
            });
        }
        
        function sortByColumn(field) {
            if (currentSort.field === field) {
                currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.field = field;
                currentSort.dir = 'desc';
            }
            
            document.querySelectorAll('th.sortable').forEach(th => {
                th.classList.remove('sort-asc', 'sort-desc');
                th.textContent = th.textContent.replace(/[▲▼]/g, '').trim();
            });
            
            const activeTh = document.querySelector('th[data-sort=\"' + field + '\"]');
            if (activeTh) {
                activeTh.classList.add(currentSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
                activeTh.textContent += ' ' + (currentSort.dir === 'asc' ? '▲' : '▼');
            }
            
            currentPage = 1;
            fetchPaginatedRecords();
        }
        
        function toggleAllApps() {
            showingAllApps = !showingAllApps;
            const btn = document.getElementById('viewAllAppsBtn');
            const container = document.getElementById('appsChartContainer');
            
            if (showingAllApps) {
                btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg> Show Less';
                container.style.height = '600px';
            } else {
                btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg> View All';
                container.style.height = '420px';
            }
            
            updateAppsChart(allAppsData);
        }
        
        function updateAppsChart(topApps) {
            const displayApps = showingAllApps ? topApps.slice(0, 30) : topApps.slice(0, 15);
            const colors = displayApps.map((_, i) => appBarColors[i % appBarColors.length]);
            
            if (charts.apps) charts.apps.destroy();
            charts.apps = new Chart(document.getElementById('appsChart'), {
                type: 'bar',
                data: {
                    labels: displayApps.map(a => a.app),
                    datasets: [{
                        label: 'Installations',
                        data: displayApps.map(a => a.count),
                        backgroundColor: colors,
                        borderRadius: 6,
                        borderSkipped: false
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    indexAxis: 'x',
                    plugins: { 
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(21, 27, 35, 0.95)',
                            titleColor: '#e2e8f0',
                            bodyColor: '#e2e8f0',
                            borderColor: '#2d3748',
                            borderWidth: 1,
                            padding: 12,
                            displayColors: true,
                            callbacks: {
                                label: function(ctx) {
                                    return ctx.parsed.y.toLocaleString() + ' installations';
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { 
                                color: '#8b949e',
                                maxRotation: 45,
                                minRotation: 45
                            },
                            grid: { display: false }
                        },
                        y: {
                            beginAtZero: true,
                            ticks: { 
                                color: '#8b949e',
                                callback: function(value) { 
                                    if (value >= 1000) return (value/1000).toFixed(0) + 'k';
                                    return value;
                                }
                            },
                            grid: { color: '#2d3748' }
                        }
                    }
                }
            });
        }
        
        function updateCharts(data) {
            // Daily chart
            if (charts.daily) charts.daily.destroy();
            charts.daily = new Chart(document.getElementById('dailyChart'), {
                type: 'line',
                data: {
                    labels: data.daily_stats.map(d => d.date.slice(5)),
                    datasets: [
                        {
                            label: 'Success',
                            data: data.daily_stats.map(d => d.success),
                            borderColor: '#22c55e',
                            backgroundColor: 'rgba(34, 197, 94, 0.1)',
                            fill: true,
                            tension: 0.4,
                            borderWidth: 2
                        },
                        {
                            label: 'Failed',
                            data: data.daily_stats.map(d => d.failed),
                            borderColor: '#ef4444',
                            backgroundColor: 'rgba(239, 68, 68, 0.1)',
                            fill: true,
                            tension: 0.4,
                            borderWidth: 2
                        }
                    ]
                },
                options: {
                    ...chartDefaults,
                    plugins: { legend: { display: true, position: 'top', labels: { color: '#8b949e', usePointStyle: true } } }
                }
            });
            
            // OS distribution - horizontal bar chart
            if (charts.os) charts.os.destroy();
            charts.os = new Chart(document.getElementById('osChart'), {
                type: 'bar',
                data: {
                    labels: data.os_distribution.map(o => o.os),
                    datasets: [{
                        data: data.os_distribution.map(o => o.count),
                        backgroundColor: appBarColors.slice(0, data.os_distribution.length),
                        borderRadius: 4,
                        borderSkipped: false
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    indexAxis: 'y',
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function(ctx) {
                                    return ctx.parsed.x.toLocaleString() + ' installations';
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#8b949e',
                                callback: function(v) { return v >= 1000 ? (v/1000).toFixed(0) + 'k' : v; }
                            },
                            grid: { color: '#2d3748' }
                        },
                        y: {
                            ticks: { color: '#8b949e' },
                            grid: { display: false }
                        }
                    }
                }
            });
            
            // Status pie chart
            if (charts.status) charts.status.destroy();
            charts.status = new Chart(document.getElementById('statusChart'), {
                type: 'doughnut',
                data: {
                    labels: ['Success', 'Failed', 'Installing'],
                    datasets: [{
                        data: [data.success_count, data.failed_count, data.installing_count],
                        backgroundColor: ['#22c55e', '#ef4444', '#eab308'],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'right', labels: { color: '#8b949e', padding: 12 } }
                    }
                }
            });
            
            // Top apps chart
            updateAppsChart(data.top_apps || []);
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
        
        function changePerPage() {
            perPage = parseInt(document.getElementById('perPageSelect').value);
            currentPage = 1;
            fetchPaginatedRecords();
        }
        
        async function fetchPaginatedRecords() {
            const status = document.getElementById('filterStatus').value;
            const app = document.getElementById('filterApp').value;
            const os = document.getElementById('filterOs').value;
            const type = document.getElementById('filterType').value;
            
            try {
                const activeBtn = document.querySelector('.filter-btn.active');
                const days = activeBtn ? activeBtn.dataset.days : '1';
                const repo = document.querySelector('.source-btn.active')?.dataset.repo || 'ProxmoxVE';
                
                let url = '/api/records?page=' + currentPage + '&limit=' + perPage + '&days=' + days + '&repo=' + encodeURIComponent(repo);
                if (status) url += '&status=' + encodeURIComponent(status);
                if (app) url += '&app=' + encodeURIComponent(app);
                if (os) url += '&os=' + encodeURIComponent(os);
                if (type) url += '&type=' + encodeURIComponent(type);
                if (currentSort.field) {
                    url += '&sort=' + (currentSort.dir === 'desc' ? '-' : '') + currentSort.field;
                }
                
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
        
        // Store current records for detail view
        let currentRecords = [];
        
        function renderTableRows(records) {
            const tbody = document.getElementById('recordsTable');
            currentRecords = records;
            
            if (records.length === 0) {
                tbody.innerHTML = '<tr><td colspan="8"><div class="loading" style="padding: 40px;">No records found</div></td></tr>';
                return;
            }
            
            tbody.innerHTML = records.map((r, index) => {
                const statusClass = r.status || 'unknown';
                const typeClass = (r.type || '').toLowerCase();
                const diskSize = r.disk_size ? r.disk_size + 'GB' : '-';
                const coreCount = r.core_count || '-';
                const ramSize = r.ram_size ? r.ram_size + 'MB' : '-';
                const created = r.created ? formatTimestamp(r.created) : '-';
                const osDisplay = r.os_type ? (r.os_type + (r.os_version ? ' ' + r.os_version : '')) : '-';
                
                return '<tr class="clickable-row" onclick="showRecordDetail(' + index + ')">' +
                    '<td><span class="status-badge ' + statusClass + '">' + escapeHtml(r.status || 'unknown') + '</span></td>' +
                    '<td><span class="type-badge ' + typeClass + '">' + escapeHtml((r.type || '-').toUpperCase()) + '</span></td>' +
                    '<td><strong>' + escapeHtml(r.nsapp || '-') + '</strong></td>' +
                    '<td>' + escapeHtml(osDisplay) + '</td>' +
                    '<td>' + diskSize + '</td>' +
                    '<td style="text-align: center;">' + coreCount + '</td>' +
                    '<td>' + ramSize + '</td>' +
                    '<td>' + created + '</td>' +
                '</tr>';
            }).join('');
        }
        
        function showRecordDetail(index) {
            const record = currentRecords[index];
            if (!record) return;
            
            const modal = document.getElementById('detailModal');
            const modalTitle = document.getElementById('modalTitle').querySelector('span');
            const modalBody = document.getElementById('modalBody');
            
            modalTitle.textContent = record.nsapp || 'Record Details';
            
            // Build detail content with sections
            let html = '';
            
            // General Information Section
            html += '<div class="detail-section">';
            html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg> General Information</div>';
            html += '<div class="detail-grid">';
            html += buildDetailItem('App Name', record.nsapp);
            html += buildDetailItem('Status', record.status, 'status-' + (record.status || 'unknown'));
            html += buildDetailItem('Type', formatType(record.type));
            html += buildDetailItem('Method', record.method || 'default');
            html += buildDetailItem('Random ID', record.random_id, 'mono');
            html += '</div></div>';
            
            // System Resources Section
            html += '<div class="detail-section">';
            html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2" ry="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg> System Resources</div>';
            html += '<div class="detail-grid">';
            html += buildDetailItem('CPU Cores', record.core_count ? record.core_count + ' Cores' : null);
            html += buildDetailItem('RAM', record.ram_size ? formatBytes(record.ram_size * 1024 * 1024) : null);
            html += buildDetailItem('Disk Size', record.disk_size ? record.disk_size + ' GB' : null);
            html += buildDetailItem('CT Type', record.ct_type !== undefined ? (record.ct_type === 1 ? 'Unprivileged' : 'Privileged') : null);
            html += '</div></div>';
            
            // Operating System Section
            html += '<div class="detail-section">';
            html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg> Operating System</div>';
            html += '<div class="detail-grid">';
            html += buildDetailItem('OS Type', record.os_type);
            html += buildDetailItem('OS Version', record.os_version);
            html += buildDetailItem('PVE Version', record.pve_version);
            html += '</div></div>';
            
            // Hardware Section (CPU & GPU)
            const hasHardwareInfo = record.cpu_vendor || record.cpu_model || record.gpu_vendor || record.gpu_model || record.ram_speed;
            if (hasHardwareInfo) {
                html += '<div class="detail-section">';
                html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg> Hardware</div>';
                html += '<div class="detail-grid">';
                html += buildDetailItem('CPU Vendor', record.cpu_vendor);
                html += buildDetailItem('CPU Model', record.cpu_model);
                html += buildDetailItem('RAM Speed', record.ram_speed);
                html += buildDetailItem('GPU Vendor', record.gpu_vendor);
                html += buildDetailItem('GPU Model', record.gpu_model);
                html += buildDetailItem('GPU Passthrough', formatPassthrough(record.gpu_passthrough));
                html += '</div></div>';
            }
            
            // Installation Details Section
            html += '<div class="detail-section">';
            html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Installation</div>';
            html += '<div class="detail-grid">';
            html += buildDetailItem('Exit Code', record.exit_code !== undefined ? record.exit_code : null, record.exit_code === 0 ? 'status-success' : (record.exit_code ? 'status-failed' : ''));
            html += buildDetailItem('Duration', record.install_duration ? formatDuration(record.install_duration) : null);
            html += buildDetailItem('Error Category', record.error_category);
            html += '</div></div>';
            
            // Error Section (if present)
            if (record.error) {
                html += '<div class="detail-section">';
                html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Error Details</div>';
                html += '<div class="error-box">' + escapeHtml(record.error) + '</div>';
                html += '</div>';
            }
            
            // Timestamps Section
            html += '<div class="detail-section">';
            html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg> Timestamps</div>';
            html += '<div class="detail-grid">';
            html += buildDetailItem('Created', formatFullTimestamp(record.created));
            html += buildDetailItem('Updated', formatFullTimestamp(record.updated));
            html += '</div></div>';
            
            modalBody.innerHTML = html;
            modal.classList.add('active');
            document.body.style.overflow = 'hidden';
        }
        
        function buildDetailItem(label, value, extraClass) {
            if (value === null || value === undefined || value === '') {
                return '<div class="detail-item"><div class="label">' + escapeHtml(label) + '</div><div class="value" style="color: var(--text-secondary);">—</div></div>';
            }
            const valueClass = extraClass ? 'value ' + extraClass : 'value';
            return '<div class="detail-item"><div class="label">' + escapeHtml(label) + '</div><div class="' + valueClass + '">' + escapeHtml(String(value)) + '</div></div>';
        }
        
        function formatType(type) {
            if (!type) return null;
            const types = {
                'lxc': 'LXC Container',
                'vm': 'Virtual Machine',
                'addon': 'Add-on',
                'pve': 'Proxmox VE',
                'tool': 'Tool'
            };
            return types[type.toLowerCase()] || type;
        }
        
        function formatPassthrough(pt) {
            if (!pt) return null;
            const modes = {
                'igpu': 'Integrated GPU',
                'dgpu': 'Dedicated GPU',
                'vgpu': 'Virtual GPU',
                'none': 'None',
                'unknown': 'Unknown'
            };
            return modes[pt.toLowerCase()] || pt;
        }
        
        function formatBytes(bytes) {
            if (!bytes) return null;
            const gb = bytes / (1024 * 1024 * 1024);
            if (gb >= 1) return gb.toFixed(1) + ' GB';
            const mb = bytes / (1024 * 1024);
            return mb.toFixed(0) + ' MB';
        }
        
        function formatDuration(seconds) {
            if (!seconds) return null;
            if (seconds < 60) return seconds + 's';
            const mins = Math.floor(seconds / 60);
            const secs = seconds % 60;
            if (mins < 60) return mins + 'm ' + secs + 's';
            const hours = Math.floor(mins / 60);
            const remainMins = mins % 60;
            return hours + 'h ' + remainMins + 'm';
        }
        
        function formatFullTimestamp(ts) {
            if (!ts) return null;
            const d = new Date(ts);
            return d.toLocaleDateString() + ' ' + d.toLocaleTimeString();
        }
        
        function closeModal() {
            const modal = document.getElementById('detailModal');
            modal.classList.remove('active');
            document.body.style.overflow = '';
        }
        
        function closeModalOutside(event) {
            if (event.target === document.getElementById('detailModal')) {
                closeModal();
            }
        }
        
        // Close modal with Escape key
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                closeModal();
                closeHealthModal();
            }
        });
        
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
        
        async function showHealthCheck() {
            const modal = document.getElementById('healthModal');
            const body = document.getElementById('healthModalBody');
            body.innerHTML = '<div class="loading">Checking...</div>';
            modal.classList.add('active');
            document.body.style.overflow = 'hidden';
            
            try {
                const resp = await fetch('/healthz');
                const data = await resp.json();
                
                const isOk = data.status === 'ok';
                const statusClass = isOk ? 'ok' : 'error';
                const icon = isOk ? '✅' : '❌';
                const title = isOk ? 'All Systems Operational' : 'Service Degraded';
                
                let html = '<div class="health-status ' + statusClass + '">';
                html += '<span class="icon">' + icon + '</span>';
                html += '<div class="details">';
                html += '<div class="title">' + title + '</div>';
                html += '<div class="subtitle">Last checked: ' + new Date().toLocaleTimeString() + '</div>';
                html += '</div></div>';
                
                html += '<div class="health-info">';
                html += '<div><span>Status</span><span>' + data.status + '</span></div>';
                html += '<div><span>Server Time</span><span>' + new Date(data.time).toLocaleString() + '</span></div>';
                if (data.pocketbase) {
                    html += '<div><span>PocketBase</span><span>' + (data.pocketbase === 'connected' ? '🟢 Connected' : '🔴 ' + data.pocketbase) + '</span></div>';
                }
                if (data.version) {
                    html += '<div><span>Version</span><span>' + data.version + '</span></div>';
                }
                html += '</div>';
                
                body.innerHTML = html;
            } catch (e) {
                body.innerHTML = '<div class="health-status error"><span class="icon">❌</span><div class="details"><div class="title">Connection Failed</div><div class="subtitle">' + e.message + '</div></div></div>';
            }
        }
        
        function closeHealthModal(event) {
            if (event && event.target !== document.getElementById('healthModal')) return;
            document.getElementById('healthModal').classList.remove('active');
            document.body.style.overflow = '';
        }
        
        async function refreshData() {
            try {
                const data = await fetchData();
                updateStats(data);
                updateCharts(data);
                // Refresh paginated Installation Log with current filters (NOT from cached recent_records)
                currentPage = 1;
                fetchPaginatedRecords();
            } catch (e) {
                console.error(e);
            }
        }
        
        // Initial load
        refreshData();
        initSortableHeaders();
        
        // Source button clicks
        document.querySelectorAll('.source-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.source-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                refreshData();
            });
        });

        // Quickfilter button clicks
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                refreshData();
            });
        });
        
        // Auto-refresh functionality
        function toggleAutoRefresh() {
            autoRefreshEnabled = document.getElementById('autoRefreshToggle').checked;
            localStorage.setItem('autoRefresh', autoRefreshEnabled);
            
            const intervalDisplay = document.getElementById('refreshInterval');
            
            if (autoRefreshEnabled) {
                intervalDisplay.classList.add('active');
                startAutoRefresh();
            } else {
                intervalDisplay.classList.remove('active');
                stopAutoRefresh();
            }
        }
        
        function startAutoRefresh() {
            stopAutoRefresh(); // Clear any existing timer
            
            let countdown = autoRefreshInterval / 1000;
            const intervalDisplay = document.getElementById('refreshInterval');
            
            // Update countdown display
            const countdownTimer = setInterval(() => {
                countdown--;
                if (countdown <= 0) {
                    countdown = autoRefreshInterval / 1000;
                }
                intervalDisplay.textContent = countdown + 's';
            }, 1000);
            
            // Actual refresh
            autoRefreshTimer = setInterval(() => {
                refreshData();
                countdown = autoRefreshInterval / 1000;
            }, autoRefreshInterval);
            
            // Store countdown timer for cleanup
            autoRefreshTimer.countdownTimer = countdownTimer;
        }
        
        function stopAutoRefresh() {
            if (autoRefreshTimer) {
                clearInterval(autoRefreshTimer);
                if (autoRefreshTimer.countdownTimer) {
                    clearInterval(autoRefreshTimer.countdownTimer);
                }
                autoRefreshTimer = null;
            }
            document.getElementById('refreshInterval').textContent = '15s';
        }
        
        // Initialize auto-refresh state on load
        document.getElementById('autoRefreshToggle').checked = autoRefreshEnabled;
        if (autoRefreshEnabled) {
            document.getElementById('refreshInterval').classList.add('active');
            startAutoRefresh();
        }
    </script>
</body>
</html>`
}

// ErrorAnalysisHTML returns the dedicated error analysis page
func ErrorAnalysisHTML() string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Error Analysis - Proxmox VE Helper-Scripts</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🔍</text></svg>">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e14;
            --bg-secondary: #131920;
            --bg-tertiary: #1a2029;
            --bg-card: #151b23;
            --border-color: #2d3748;
            --text-primary: #e2e8f0;
            --text-secondary: #8b949e;
            --text-muted: #64748b;
            --accent-blue: #3b82f6;
            --accent-cyan: #22d3ee;
            --accent-green: #22c55e;
            --accent-red: #ef4444;
            --accent-yellow: #eab308;
            --accent-orange: #f97316;
            --accent-purple: #a855f7;
            --accent-pink: #ec4899;
        }
        [data-theme="light"] {
            --bg-primary: #f8fafc; --bg-secondary: #ffffff; --bg-tertiary: #f1f5f9;
            --bg-card: #ffffff; --border-color: #e2e8f0; --text-primary: #1e293b;
            --text-secondary: #64748b; --text-muted: #94a3b8;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Inter', sans-serif; background: var(--bg-primary); color: var(--text-primary); min-height: 100vh; }
        .navbar { background: var(--bg-secondary); border-bottom: 1px solid var(--border-color); padding: 0 24px; height: 64px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; }
        .navbar-brand { display: flex; align-items: center; gap: 12px; text-decoration: none; color: var(--text-primary); font-weight: 600; }
        .navbar-brand svg { color: var(--accent-cyan); }
        .nav-links { display: flex; gap: 8px; }
        .nav-link { padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 500; color: var(--text-secondary); text-decoration: none; transition: all 0.2s; border: 1px solid transparent; }
        .nav-link:hover { background: var(--bg-tertiary); color: var(--text-primary); }
        .nav-link.active { background: var(--accent-blue); color: #fff; border-color: var(--accent-blue); }
        .main-content { padding: 24px 40px; max-width: 1920px; margin: 0 auto; }
        .page-header { margin-bottom: 24px; }
        .page-header h1 { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
        .page-header p { color: var(--text-secondary); font-size: 15px; }
        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 24px; }
        .stat-card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; }
        .stat-card .label { font-size: 13px; color: var(--text-secondary); margin-bottom: 8px; }
        .stat-card .value { font-size: 32px; font-weight: 700; }
        .stat-card .sub { font-size: 12px; color: var(--text-muted); margin-top: 4px; }
        .stat-card.red .value { color: var(--accent-red); }
        .stat-card.yellow .value { color: var(--accent-yellow); }
        .stat-card.orange .value { color: var(--accent-orange); }
        .stat-card.purple .value { color: var(--accent-purple); }
        .section-card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; margin-bottom: 24px; overflow: hidden; }
        .section-header { display: flex; justify-content: space-between; align-items: center; padding: 20px 24px; border-bottom: 1px solid var(--border-color); }
        .section-header h2 { font-size: 18px; font-weight: 600; }
        .section-header p { font-size: 13px; color: var(--text-secondary); margin-top: 2px; }
        .filters-bar { display: flex; align-items: center; gap: 16px; padding: 16px 24px; background: var(--bg-tertiary); border-bottom: 1px solid var(--border-color); flex-wrap: wrap; }
        .filter-group { display: flex; align-items: center; gap: 8px; }
        .filter-group label { font-size: 13px; color: var(--text-secondary); white-space: nowrap; }
        .filter-divider { width: 1px; height: 32px; background: var(--border-color); }
        .quickfilter { display: flex; gap: 4px; background: var(--bg-secondary); padding: 4px; border-radius: 8px; border: 1px solid var(--border-color); }
        .filter-btn { background: transparent; border: none; color: var(--text-secondary); padding: 6px 14px; border-radius: 6px; font-size: 13px; font-weight: 500; cursor: pointer; transition: all 0.2s; }
        .filter-btn:hover { background: var(--bg-tertiary); color: var(--text-primary); }
        .filter-btn.active { background: var(--accent-blue); color: #fff; }
        .source-btn { background: transparent; border: none; color: var(--text-secondary); padding: 6px 14px; border-radius: 6px; font-size: 13px; font-weight: 500; cursor: pointer; transition: all 0.2s; }
        .source-btn:hover { background: var(--bg-tertiary); color: var(--text-primary); }
        .source-btn.active { background: var(--accent-green); color: #fff; }
        .btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 500; cursor: pointer; transition: all 0.2s; border: 1px solid var(--border-color); background: var(--bg-tertiary); color: var(--text-primary); }
        .btn:hover { background: var(--bg-primary); border-color: var(--accent-blue); }
        .btn-danger { background: var(--accent-red); border-color: var(--accent-red); color: #fff; }
        .btn-danger:hover { background: #dc2626; }
        .btn-primary { background: var(--accent-blue); border-color: var(--accent-blue); color: #fff; }
        .btn-primary:hover { background: #2563eb; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; }
        th { font-size: 12px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.5px; background: var(--bg-tertiary); border-bottom: 1px solid var(--border-color); white-space: nowrap; }
        td { font-size: 13px; border-bottom: 1px solid var(--border-color); }
        tr:hover td { background: rgba(59,130,246,0.05); }
        .status-badge { display: inline-flex; align-items: center; padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 600; text-transform: capitalize; }
        .status-badge.failed { background: rgba(239,68,68,0.15); color: var(--accent-red); }
        .status-badge.aborted { background: rgba(168,85,247,0.15); color: var(--accent-purple); }
        .type-badge { display: inline-flex; padding: 2px 8px; border-radius: 6px; font-size: 10px; font-weight: 700; text-transform: uppercase; }
        .type-badge.lxc { background: rgba(34,211,238,0.15); color: var(--accent-cyan); }
        .type-badge.vm { background: rgba(168,85,247,0.15); color: var(--accent-purple); }
        .exit-code { font-family: 'Consolas', monospace; font-size: 13px; font-weight: 600; padding: 2px 8px; border-radius: 4px; }
        .exit-code.err { background: rgba(239,68,68,0.15); color: var(--accent-red); }
        .exit-code.ok { background: rgba(34,197,94,0.15); color: var(--accent-green); }
        .error-text { font-family: 'Consolas', monospace; font-size: 12px; color: var(--accent-red); max-width: 400px; word-break: break-word; }
        .category-badge { display: inline-flex; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
        .category-badge.apt { background: rgba(239,68,68,0.15); color: var(--accent-red); }
        .category-badge.network { background: rgba(59,130,246,0.15); color: var(--accent-blue); }
        .category-badge.permission { background: rgba(249,115,22,0.15); color: var(--accent-orange); }
        .category-badge.command_not_found { background: rgba(168,85,247,0.15); color: var(--accent-purple); }
        .category-badge.user_aborted { background: rgba(100,116,139,0.15); color: var(--text-muted); }
        .category-badge.timeout { background: rgba(234,179,8,0.15); color: var(--accent-yellow); }
        .category-badge.storage { background: rgba(236,72,153,0.15); color: var(--accent-pink); }
        .category-badge.resource { background: rgba(249,115,22,0.15); color: var(--accent-orange); }
        .category-badge.dependency { background: rgba(34,211,238,0.15); color: var(--accent-cyan); }
        .category-badge.signal { background: rgba(234,179,8,0.15); color: var(--accent-yellow); }
        .category-badge.config { background: rgba(132,204,22,0.15); color: #84cc16; }
        .category-badge.unknown, .category-badge.uncategorized { background: rgba(100,116,139,0.15); color: var(--text-muted); }
        .charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
        .chart-card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; }
        .chart-card h3 { font-size: 14px; font-weight: 600; margin-bottom: 16px; color: var(--text-secondary); }
        .chart-wrapper { height: 280px; }
        .loading { display: flex; justify-content: center; align-items: center; padding: 60px; color: var(--text-secondary); }
        .loading-spinner { width: 40px; height: 40px; border: 3px solid var(--border-color); border-top-color: var(--accent-blue); border-radius: 50%; animation: spin 1s linear infinite; margin-right: 12px; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .progress-bar { height: 8px; background: var(--bg-tertiary); border-radius: 4px; overflow: hidden; margin-top: 6px; }
        .progress-bar-fill { height: 100%; border-radius: 4px; transition: width 0.5s; }
        .modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.75); display: flex; justify-content: center; align-items: center; z-index: 1000; opacity: 0; visibility: hidden; transition: opacity 0.2s, visibility 0.2s; }
        .modal-overlay.active { opacity: 1; visibility: visible; }
        .modal-content { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 16px; width: 90%; max-width: 600px; max-height: 80vh; overflow-y: auto; }
        .modal-header { display: flex; justify-content: space-between; align-items: center; padding: 20px 24px; border-bottom: 1px solid var(--border-color); }
        .modal-header h2 { font-size: 18px; font-weight: 600; }
        .modal-close { background: none; border: none; color: var(--text-secondary); font-size: 20px; cursor: pointer; padding: 4px 8px; border-radius: 4px; }
        .modal-close:hover { background: var(--bg-tertiary); }
        .modal-body { padding: 24px; }
        .form-group { margin-bottom: 16px; }
        .form-group label { display: block; font-size: 13px; color: var(--text-secondary); margin-bottom: 6px; font-weight: 500; }
        .form-input { width: 100%; background: var(--bg-tertiary); border: 1px solid var(--border-color); color: var(--text-primary); padding: 10px 14px; border-radius: 8px; font-size: 14px; outline: none; }
        .form-input:focus { border-color: var(--accent-blue); }
        textarea.form-input { min-height: 120px; resize: vertical; font-family: 'Consolas', monospace; font-size: 12px; }
        .alert-box { padding: 12px 16px; border-radius: 8px; font-size: 13px; margin-bottom: 16px; }
        .alert-box.success { background: rgba(34,197,94,0.15); color: var(--accent-green); border: 1px solid rgba(34,197,94,0.3); }
        .alert-box.error { background: rgba(239,68,68,0.15); color: var(--accent-red); border: 1px solid rgba(239,68,68,0.3); }
        .alert-box.warning { background: rgba(234,179,8,0.15); color: var(--accent-yellow); border: 1px solid rgba(234,179,8,0.3); }
        .stuck-banner { background: rgba(234,179,8,0.1); border: 1px solid rgba(234,179,8,0.3); color: var(--accent-yellow); padding: 16px 24px; border-radius: 12px; margin-bottom: 24px; display: flex; align-items: center; justify-content: space-between; }
        .table-wrapper { overflow-x: auto; }
        @media (max-width: 1200px) { .stats-grid { grid-template-columns: repeat(2, 1fr); } .charts-grid { grid-template-columns: 1fr; } }
        @media (max-width: 768px) { .stats-grid { grid-template-columns: 1fr; } .main-content { padding: 16px; } }
    </style>
</head>
<body>
    <nav class="navbar">
        <a href="/" class="navbar-brand">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>
            Proxmox VE Helper-Scripts
        </a>
        <div class="nav-links">
            <a href="/" class="nav-link">📊 Dashboard</a>
            <a href="/error-analysis" class="nav-link active">🔍 Error Analysis</a>
        </div>
    </nav>

    <div class="main-content">
        <div class="page-header">
            <h1>🔍 Error Analysis</h1>
            <p>Detailed error and failure analysis with exit code breakdowns.</p>
        </div>

        <!-- Filters -->
        <div class="filters-bar" style="background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; margin-bottom: 24px;">
            <div class="filter-group">
                <label>Source:</label>
                <div class="quickfilter">
                    <button class="source-btn active" data-repo="ProxmoxVE">ProxmoxVE</button>
                    <button class="source-btn" data-repo="ProxmoxVED">ProxmoxVED</button>
                    <button class="source-btn" data-repo="all">All</button>
                </div>
            </div>
            <div class="filter-divider"></div>
            <div class="filter-group">
                <label>Period:</label>
                <div class="quickfilter">
                    <button class="filter-btn active" data-days="1">Today</button>
                    <button class="filter-btn" data-days="7">7 Days</button>
                    <button class="filter-btn" data-days="30">30 Days</button>
                    <button class="filter-btn" data-days="90">90 Days</button>
                </div>
            </div>
            <div style="margin-left: auto;">
                <button class="btn" onclick="refreshData()">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6"/><path d="M1 20v-6h6"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>
                    Refresh
                </button>
            </div>
        </div>

        <!-- Stuck Installing Banner -->
        <div id="stuckBanner" class="stuck-banner" style="display: none;">
            <div>
                <strong>⚠️ <span id="stuckCount">0</span> installations stuck in "Installing" state</strong>
                <div style="font-size: 12px; margin-top: 4px;">These records have not received a completion status. Cleanup runs every 15 min for records older than 2h.</div>
            </div>
            <button class="btn btn-danger" onclick="triggerCleanup()">🧹 Run Cleanup Now</button>
        </div>

        <!-- Stats Cards -->
        <div class="stats-grid">
            <div class="stat-card red">
                <div class="label">Total Errors</div>
                <div class="value" id="totalErrors">-</div>
                <div class="sub">failed + aborted</div>
            </div>
            <div class="stat-card orange">
                <div class="label">Overall Failure Rate</div>
                <div class="value" id="failRate">-</div>
                <div class="sub">of all installations</div>
            </div>
            <div class="stat-card yellow">
                <div class="label">Stuck Installing</div>
                <div class="value" id="stuckCount2">-</div>
                <div class="sub">no completion received</div>
            </div>
            <div class="stat-card purple">
                <div class="label">Total Installations</div>
                <div class="value" id="totalInstalls">-</div>
                <div class="sub">in selected period</div>
            </div>
        </div>

        <!-- Charts -->
        <div class="charts-grid">
            <div class="chart-card">
                <h3>Error Timeline</h3>
                <div class="chart-wrapper"><canvas id="timelineChart"></canvas></div>
            </div>
            <div class="chart-card">
                <h3>Error Categories</h3>
                <div class="chart-wrapper"><canvas id="categoryChart"></canvas></div>
            </div>
        </div>

        <!-- Exit Code Distribution -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Exit Code Distribution</h2>
                    <p>Breakdown of all exit codes returned by failed scripts.</p>
                </div>
            </div>
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Exit Code</th>
                            <th>Description</th>
                            <th>Category</th>
                            <th>Count</th>
                            <th>Percentage</th>
                            <th>Distribution</th>
                        </tr>
                    </thead>
                    <tbody id="exitCodeTable">
                        <tr><td colspan="6"><div class="loading"><div class="loading-spinner"></div>Loading...</div></td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Error Categories -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Error Categories</h2>
                    <p>Grouped errors by type with affected applications.</p>
                </div>
            </div>
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Category</th>
                            <th>Count</th>
                            <th>Percentage</th>
                            <th>Affected Apps</th>
                        </tr>
                    </thead>
                    <tbody id="categoryTable">
                        <tr><td colspan="4"><div class="loading"><div class="loading-spinner"></div>Loading...</div></td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Apps with Errors -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Applications with Errors</h2>
                    <p>All apps that experienced failures, sorted by error count.</p>
                </div>
                <div style="display: flex; gap: 8px;">
                    <input type="text" id="appFilter" placeholder="Filter by app name..." style="background: var(--bg-secondary); border: 1px solid var(--border-color); color: var(--text-primary); padding: 8px 14px; border-radius: 8px; font-size: 13px; outline: none; width: 200px;" oninput="filterAppTable()">
                </div>
            </div>
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Application</th>
                            <th>Type</th>
                            <th>Total</th>
                            <th>Failed</th>
                            <th>Aborted</th>
                            <th>Fail Rate</th>
                            <th>Top Exit Code</th>
                            <th>Top Error</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="appErrorTable">
                        <tr><td colspan="9"><div class="loading"><div class="loading-spinner"></div>Loading...</div></td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Recent Error Log -->
        <div class="section-card">
            <div class="section-header">
                <div>
                    <h2>Recent Error Log</h2>
                    <p>Last 100 failed or aborted installations.</p>
                </div>
            </div>
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Type</th>
                            <th>Application</th>
                            <th>Exit Code</th>
                            <th>Category</th>
                            <th>Error</th>
                            <th>OS</th>
                            <th>Timestamp</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="recentErrorTable">
                        <tr><td colspan="9"><div class="loading"><div class="loading-spinner"></div>Loading...</div></td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- GitHub Issue Modal -->
    <div class="modal-overlay" id="issueModal" onclick="if(event.target===this)closeIssueModal()">
        <div class="modal-content" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h2>🐛 Create GitHub Issue</h2>
                <button class="modal-close" onclick="closeIssueModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div id="issueAlert" class="alert-box" style="display: none;"></div>
                <div class="form-group">
                    <label>Admin Password *</label>
                    <input type="password" id="issuePassword" class="form-input" placeholder="Enter admin password...">
                </div>
                <div class="form-group">
                    <label>Issue Title</label>
                    <input type="text" id="issueTitle" class="form-input">
                </div>
                <div class="form-group">
                    <label>Issue Body</label>
                    <textarea id="issueBody" class="form-input"></textarea>
                </div>
                <div class="form-group">
                    <label>Labels (comma-separated)</label>
                    <input type="text" id="issueLabels" class="form-input" value="bug, telemetry">
                </div>
                <div style="display: flex; gap: 8px; justify-content: flex-end; margin-top: 20px;">
                    <button class="btn" onclick="closeIssueModal()">Cancel</button>
                    <button class="btn btn-primary" id="submitIssueBtn" onclick="submitIssue()">Create Issue</button>
                </div>
            </div>
        </div>
    </div>

    <!-- Cleanup Modal -->
    <div class="modal-overlay" id="cleanupModal" onclick="if(event.target===this)closeCleanupModal()">
        <div class="modal-content" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h2>🧹 Trigger Cleanup</h2>
                <button class="modal-close" onclick="closeCleanupModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div id="cleanupAlert" class="alert-box" style="display: none;"></div>
                <p style="margin-bottom: 16px; color: var(--text-secondary);">This will mark all stuck "Installing" records (older than 2h) as "unknown". This action requires the admin password.</p>
                <div class="form-group">
                    <label>Admin Password *</label>
                    <input type="password" id="cleanupPassword" class="form-input" placeholder="Enter admin password...">
                </div>
                <div style="display: flex; gap: 8px; justify-content: flex-end; margin-top: 20px;">
                    <button class="btn" onclick="closeCleanupModal()">Cancel</button>
                    <button class="btn btn-danger" id="runCleanupBtn" onclick="runCleanup()">Run Cleanup</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        let charts = {};
        let currentData = null;
        let currentTheme = localStorage.getItem('theme') || 'dark';
        if (currentTheme === 'light') document.documentElement.setAttribute('data-theme', 'light');

        const catColors = {
            'apt': '#ef4444', 'network': '#3b82f6', 'permission': '#f97316',
            'command_not_found': '#a855f7', 'user_aborted': '#64748b', 'timeout': '#eab308',
            'storage': '#ec4899', 'resource': '#f97316', 'dependency': '#22d3ee',
            'signal': '#eab308', 'config': '#84cc16', 'unknown': '#64748b',
            'uncategorized': '#94a3b8'
        };

        function escapeHtml(str) {
            if (!str) return '';
            return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        }

        function escapeAttr(str) {
            if (!str) return '';
            return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/\n/g,'&#10;').replace(/\r/g,'&#13;');
        }

        function toggleError(id) {
            var s = document.getElementById(id + '-short');
            var f = document.getElementById(id + '-full');
            if (f && s) {
                if (f.style.display === 'none') { f.style.display = 'block'; s.style.display = 'none'; }
                else { f.style.display = 'none'; s.style.display = 'block'; }
            }
        }

        function formatTimestamp(ts) {
            if (!ts) return '-';
            return new Date(ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true });
        }

        async function fetchData() {
            const days = document.querySelector('.filter-btn.active')?.dataset.days || '1';
            const repo = document.querySelector('.source-btn.active')?.dataset.repo || 'ProxmoxVE';
            try {
                const resp = await fetch('/api/errors?days=' + days + '&repo=' + repo);
                if (!resp.ok) throw new Error('Fetch failed');
                return await resp.json();
            } catch (e) {
                console.error(e);
                return null;
            }
        }

        function updateStats(data) {
            document.getElementById('totalErrors').textContent = (data.total_errors || 0).toLocaleString();
            document.getElementById('failRate').textContent = (data.overall_fail_rate || 0).toFixed(1) + '%';
            document.getElementById('stuckCount2').textContent = (data.stuck_installing || 0).toLocaleString();
            document.getElementById('totalInstalls').textContent = (data.total_installs || 0).toLocaleString();

            // Stuck banner
            if (data.stuck_installing > 0) {
                document.getElementById('stuckBanner').style.display = 'flex';
                document.getElementById('stuckCount').textContent = data.stuck_installing;
            } else {
                document.getElementById('stuckBanner').style.display = 'none';
            }
        }

        function updateExitCodeTable(exitCodes) {
            const tbody = document.getElementById('exitCodeTable');
            if (!exitCodes || exitCodes.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px;">No exit code data</td></tr>';
                return;
            }
            const maxCount = Math.max(...exitCodes.map(e => e.count));
            tbody.innerHTML = exitCodes.map(e => {
                const barWidth = (e.count / maxCount * 100).toFixed(0);
                const codeClass = e.exit_code === 0 ? 'ok' : 'err';
                const catClass = (e.category || 'unknown').replace(/ /g, '_');
                return '<tr>' +
                    '<td><span class="exit-code ' + codeClass + '">' + e.exit_code + '</span></td>' +
                    '<td>' + escapeHtml(e.description) + '</td>' +
                    '<td><span class="category-badge ' + catClass + '">' + escapeHtml(e.category) + '</span></td>' +
                    '<td><strong>' + e.count.toLocaleString() + '</strong></td>' +
                    '<td>' + e.percentage.toFixed(1) + '%</td>' +
                    '<td style="min-width:150px;"><div class="progress-bar"><div class="progress-bar-fill" style="width:' + barWidth + '%;background:var(--accent-red);"></div></div></td>' +
                '</tr>';
            }).join('');
        }

        function updateCategoryTable(categories) {
            const tbody = document.getElementById('categoryTable');
            if (!categories || categories.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:24px;">No category data</td></tr>';
                return;
            }
            tbody.innerHTML = categories.map(c => {
                const catClass = (c.category || 'unknown').replace(/ /g, '_');
                return '<tr>' +
                    '<td><span class="category-badge ' + catClass + '">' + escapeHtml(c.category) + '</span></td>' +
                    '<td><strong>' + c.count.toLocaleString() + '</strong></td>' +
                    '<td>' + c.percentage.toFixed(1) + '%</td>' +
                    '<td style="font-size:12px;color:var(--text-secondary);max-width:400px;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(c.top_apps) + '</td>' +
                '</tr>';
            }).join('');
        }

        let allAppErrors = [];
        function updateAppErrorTable(apps) {
            allAppErrors = apps || [];
            filterAppTable();
        }

        function filterAppTable() {
            const filter = (document.getElementById('appFilter').value || '').toLowerCase();
            const filtered = filter ? allAppErrors.filter(a => a.app.toLowerCase().includes(filter)) : allAppErrors;
            const tbody = document.getElementById('appErrorTable');
            if (filtered.length === 0) {
                tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px;">No matching apps</td></tr>';
                return;
            }
            tbody.innerHTML = filtered.map((a, idx) => {
                const typeClass = (a.type || '').toLowerCase();
                const failRateColor = a.failure_rate > 50 ? 'var(--accent-red)' : a.failure_rate > 20 ? 'var(--accent-orange)' : 'var(--accent-yellow)';
                const topCat = a.top_category ? '<span class="category-badge ' + a.top_category + '">' + escapeHtml(a.top_category) + '</span>' : '-';
                const errorId = 'err-app-' + idx;
                const shortError = escapeHtml((a.top_error || '-').substring(0, 80));
                const fullError = escapeHtml(a.top_error || '-');
                const isLong = (a.top_error || '').length > 80;
                return '<tr>' +
                    '<td><strong>' + escapeHtml(a.app) + '</strong></td>' +
                    '<td><span class="type-badge ' + typeClass + '">' + (a.type || '-').toUpperCase() + '</span></td>' +
                    '<td>' + a.total_count + '</td>' +
                    '<td style="color:var(--accent-red);font-weight:600;">' + a.failed_count + '</td>' +
                    '<td style="color:var(--accent-purple);">' + (a.aborted_count || 0) + '</td>' +
                    '<td style="color:' + failRateColor + ';font-weight:600;">' + a.failure_rate.toFixed(1) + '%</td>' +
                    '<td>' + (a.top_exit_code ? '<span class="exit-code err">' + a.top_exit_code + '</span>' : '-') + '</td>' +
                    '<td class="error-text">' +
                        '<div id="' + errorId + '-short">' + shortError + (isLong ? ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show more</a>' : '') + '</div>' +
                        (isLong ? '<div id="' + errorId + '-full" style="display:none;white-space:pre-wrap;word-break:break-all;max-height:300px;overflow-y:auto;">' + fullError + ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show less</a></div>' : '') +
                    '</td>' +
                    '<td><button class="btn issue-btn" data-app="' + escapeAttr(a.app) + '" data-exit="' + (a.top_exit_code||0) + '" data-error="' + escapeAttr(a.top_error||'') + '" data-rate="' + a.failure_rate.toFixed(1) + '">🐛 Issue</button></td>' +
                '</tr>';
            }).join('');
        }

        function updateRecentErrors(errors) {
            const tbody = document.getElementById('recentErrorTable');
            if (!errors || errors.length === 0) {
                tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px;">No recent errors</td></tr>';
                return;
            }
            tbody.innerHTML = errors.map((e, idx) => {
                const statusClass = e.status || 'unknown';
                const typeClass = (e.type || '').toLowerCase();
                const codeClass = e.exit_code === 0 ? 'ok' : 'err';
                const catClass = (e.error_category || 'unknown').replace(/ /g, '_');
                const os = e.os_type ? e.os_type + (e.os_version ? ' ' + e.os_version : '') : '-';
                const errorId = 'err-recent-' + idx;
                const shortError = escapeHtml((e.error || '-').substring(0, 80));
                const fullError = escapeHtml(e.error || '-');
                const isLong = (e.error || '').length > 80;
                return '<tr>' +
                    '<td><span class="status-badge ' + statusClass + '">' + escapeHtml(e.status) + '</span></td>' +
                    '<td><span class="type-badge ' + typeClass + '">' + (e.type || '-').toUpperCase() + '</span></td>' +
                    '<td><strong>' + escapeHtml(e.nsapp) + '</strong></td>' +
                    '<td><span class="exit-code ' + codeClass + '">' + e.exit_code + '</span></td>' +
                    '<td><span class="category-badge ' + catClass + '">' + escapeHtml(e.error_category || 'unknown') + '</span></td>' +
                    '<td class="error-text">' +
                        '<div id="' + errorId + '-short">' + shortError + (isLong ? ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show more</a>' : '') + '</div>' +
                        (isLong ? '<div id="' + errorId + '-full" style="display:none;white-space:pre-wrap;word-break:break-all;max-height:300px;overflow-y:auto;">' + fullError + ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show less</a></div>' : '') +
                    '</td>' +
                    '<td>' + escapeHtml(os) + '</td>' +
                    '<td style="white-space:nowrap;">' + formatTimestamp(e.created) + '</td>' +
                    '<td><button class="btn issue-btn" data-app="' + escapeAttr(e.nsapp) + '" data-exit="' + e.exit_code + '" data-error="' + escapeAttr(e.error||'') + '" data-rate="0">🐛</button></td>' +
                '</tr>';
            }).join('');
        }

        function updateCharts(data) {
            // Timeline chart
            if (charts.timeline) charts.timeline.destroy();
            const timeline = data.error_timeline || [];
            charts.timeline = new Chart(document.getElementById('timelineChart'), {
                type: 'line',
                data: {
                    labels: timeline.map(d => d.date.slice(5)),
                    datasets: [
                        { label: 'Failed', data: timeline.map(d => d.failed), borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.1)', fill: true, tension: 0.4, borderWidth: 2 },
                        { label: 'Aborted', data: timeline.map(d => d.aborted), borderColor: '#a855f7', backgroundColor: 'rgba(168,85,247,0.1)', fill: true, tension: 0.4, borderWidth: 2 }
                    ]
                },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#8b949e', usePointStyle: true } } }, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#2d3748' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#2d3748' } } } }
            });

            // Category pie chart
            if (charts.category) charts.category.destroy();
            const cats = data.category_stats || [];
            charts.category = new Chart(document.getElementById('categoryChart'), {
                type: 'doughnut',
                data: {
                    labels: cats.map(c => c.category),
                    datasets: [{ data: cats.map(c => c.count), backgroundColor: cats.map(c => catColors[c.category] || '#64748b'), borderWidth: 0 }]
                },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { color: '#8b949e', padding: 12 } } } }
            });
        }

        // GitHub Issue Modal
        function openIssueModal(app, exitCode, errorText, failRate) {
            const title = '[Telemetry] ' + app + ': Error (exit code ' + exitCode + ')';
            const fence = String.fromCharCode(96,96,96);
            const body = '## Telemetry Error Report\n\n' +
                '**Application:** ' + app + '\n' +
                '**Exit Code:** ' + exitCode + '\n' +
                '**Failure Rate:** ' + failRate + '%\n\n' +
                '### Error Details\n' + fence + '\n' + errorText + '\n' + fence + '\n\n' +
                '---\n*Created from telemetry error analysis dashboard.*';
            document.getElementById('issueTitle').value = title;
            document.getElementById('issueBody').value = body;
            document.getElementById('issueAlert').style.display = 'none';
            document.getElementById('issueModal').classList.add('active');
        }

        function closeIssueModal() {
            document.getElementById('issueModal').classList.remove('active');
        }

        async function submitIssue() {
            const btn = document.getElementById('submitIssueBtn');
            const alert = document.getElementById('issueAlert');
            const password = document.getElementById('issuePassword').value;
            if (!password) { alert.className = 'alert-box error'; alert.textContent = 'Password required'; alert.style.display = 'block'; return; }

            btn.disabled = true;
            btn.textContent = 'Creating...';

            try {
                const resp = await fetch('/api/github/create-issue', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        password: password,
                        title: document.getElementById('issueTitle').value,
                        body: document.getElementById('issueBody').value,
                        labels: document.getElementById('issueLabels').value.split(',').map(l => l.trim()).filter(Boolean)
                    })
                });
                const data = await resp.json();
                if (resp.ok && data.success) {
                    alert.className = 'alert-box success';
                    alert.innerHTML = '✅ Issue created! <a href="' + data.issue_url + '" target="_blank" style="color:var(--accent-green);">View on GitHub →</a>';
                    alert.style.display = 'block';
                } else {
                    throw new Error(data.message || resp.statusText || 'Failed');
                }
            } catch (e) {
                alert.className = 'alert-box error';
                alert.textContent = '❌ ' + e.message;
                alert.style.display = 'block';
            } finally {
                btn.disabled = false;
                btn.textContent = 'Create Issue';
            }
        }

        // Cleanup Modal
        function triggerCleanup() {
            document.getElementById('cleanupAlert').style.display = 'none';
            document.getElementById('cleanupModal').classList.add('active');
        }

        function closeCleanupModal() {
            document.getElementById('cleanupModal').classList.remove('active');
        }

        async function runCleanup() {
            const btn = document.getElementById('runCleanupBtn');
            const alert = document.getElementById('cleanupAlert');
            const password = document.getElementById('cleanupPassword').value;
            if (!password) { alert.className = 'alert-box error'; alert.textContent = 'Password required'; alert.style.display = 'block'; return; }

            btn.disabled = true;
            btn.textContent = 'Running...';

            try {
                const resp = await fetch('/api/cleanup/run', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ password: password })
                });
                const data = await resp.json();
                if (resp.ok) {
                    alert.className = 'alert-box success';
                    alert.textContent = '✅ ' + data.message;
                    alert.style.display = 'block';
                    setTimeout(() => { closeCleanupModal(); refreshData(); }, 2000);
                } else {
                    throw new Error(data.message || resp.statusText || 'Failed');
                }
            } catch (e) {
                alert.className = 'alert-box error';
                alert.textContent = '❌ ' + e.message;
                alert.style.display = 'block';
            } finally {
                btn.disabled = false;
                btn.textContent = 'Run Cleanup';
            }
        }

        async function refreshData() {
            const data = await fetchData();
            if (!data) return;
            currentData = data;
            updateStats(data);
            updateExitCodeTable(data.exit_code_stats);
            updateCategoryTable(data.category_stats);
            updateAppErrorTable(data.app_errors);
            updateRecentErrors(data.recent_errors);
            updateCharts(data);
        }

        // Filter button handling
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                refreshData();
            });
        });
        document.querySelectorAll('.source-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.source-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                refreshData();
            });
        });
        document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeIssueModal(); closeCleanupModal(); } });

        // Event delegation for Issue buttons (avoids inline onclick escaping issues)
        document.addEventListener('click', function(e) {
            var btn = e.target.closest('.issue-btn');
            if (!btn) return;
            var app = btn.getAttribute('data-app') || '';
            var exitCode = parseInt(btn.getAttribute('data-exit') || '0', 10);
            var errorText = btn.getAttribute('data-error') || '';
            var rate = parseFloat(btn.getAttribute('data-rate') || '0');
            openIssueModal(app, exitCode, errorText, rate);
        });

        // Initial load
        refreshData();
    </script>
</body>
</html>`
}