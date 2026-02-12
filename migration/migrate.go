// +build ignore

// Migration script to import data from the old API or JSON file to PocketBase
// Run with: go run migrate.go
//
// Environment variables:
//   JSON_FILE             - Path to JSON file to import (if set, skips API fetch)
//   SQL_OUTPUT            - Path to output SQL file (direct SQLite import, fastest!)
//   MIGRATION_SOURCE_URL  - Base URL of source API (default: https://api.htl-braunau.at/data)
//   POCKETBASE_URL        - PocketBase URL (default: http://localhost:8090)
//   POCKETBASE_COLLECTION - Target collection (default: telemetry)
//   PB_IDENTITY           - Auth username
//   PB_PASSWORD           - Auth password
//   REPO_SOURCE           - Value for repo_source field (e.g., "community-scripts" or "Proxmox VE")
//   DATE_UNTIL            - Only import records created before this date (format: YYYY-MM-DD)
//   DATE_FROM             - Only import records created after this date (format: YYYY-MM-DD)
//   START_PAGE            - Resume from this page (default: 1, API mode only)
//   BATCH_SIZE            - Records per batch (default: 500)
//   SKIP_RECORDS          - Skip first N records (JSON file mode, for resuming)
//   WORKERS               - Number of parallel HTTP workers (default: 50)
//
// SQL Output Mode (fastest - seconds for millions of records):
//   $env:JSON_FILE = "data.json"
//   $env:SQL_OUTPUT = "import.sql"
//   $env:REPO_SOURCE = "Proxmox VE"
//   .\migrate.exe
//   # Then on server: sqlite3 /app/pb_data/data.db < import.sql
//
// JSON file format (array of objects):
//   [{"id": "...", "ct_type": 1, "nsapp": "...", ...}, ...]
package main

import (
	"bufio"
	"bytes"
	cryptoRand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSourceAPI = "https://api.htl-braunau.at/data"
	defaultPBURL     = "http://localhost:8090"
	defaultBatchSize = 500
	defaultWorkers   = 50 // Parallel workers for importing
)

var (
	sourceAPI      string
	summaryAPI     string
	authToken      string
	repoSource     string
	dateUntil      time.Time
	dateFrom       time.Time
	hasDateUntil   bool
	hasDateFrom    bool
	workerCount    int = defaultWorkers
)

// OldDataModel represents the data structure from the old API
type OldDataModel struct {
	ID         string `json:"id"`
	CtType     int    `json:"ct_type"`
	DiskSize   int    `json:"disk_size"`
	CoreCount  int    `json:"core_count"`
	RamSize    int    `json:"ram_size"`
	OsType     string `json:"os_type"`
	OsVersion  string `json:"os_version"`
	DisableIP6 string `json:"disableip6"`
	NsApp      string `json:"nsapp"`
	Method     string `json:"method"`
	CreatedAt  string `json:"created_at"`
	PveVersion string `json:"pve_version"`
	Status     string `json:"status"`
	RandomID   string `json:"random_id"`
	Type       string `json:"type"`
	Error      string `json:"error"`
} 

// MongoNumberLong represents MongoDB's $numberLong type
type MongoNumberLong struct {
	Value string `json:"$numberLong"`
}

// MongoDate represents MongoDB's $date type
type MongoDate struct {
	Value string `json:"$date"`
}

// MongoOID represents MongoDB's $oid type
type MongoOID struct {
	Value string `json:"$oid"`
}

// MongoDataModel represents MongoDB Extended JSON export format
type MongoDataModel struct {
	MongoID    MongoOID        `json:"_id"`
	ID         json.RawMessage `json:"id"`
	CtType     json.RawMessage `json:"ct_type"`
	DiskSize   json.RawMessage `json:"disk_size"`
	CoreCount  json.RawMessage `json:"core_count"`
	RamSize    json.RawMessage `json:"ram_size"`
	OsType     string          `json:"os_type"`
	OsVersion  string          `json:"os_version"`
	DisableIP6 string          `json:"disable_ip6"`
	NsApp      string          `json:"nsapp"`
	Method     string          `json:"method"`
	CreatedAt  json.RawMessage `json:"created_at"`
	PveVersion string          `json:"pveversion"`
	Status     string          `json:"status"`
	RandomID   string          `json:"random_id"`
	Type       string          `json:"type"`
	Error      *string         `json:"error"`
}

// parseMongoInt extracts int from MongoDB $numberLong or plain number
func parseMongoInt(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	// Try $numberLong first
	var numLong MongoNumberLong
	if err := json.Unmarshal(raw, &numLong); err == nil && numLong.Value != "" {
		if n, err := strconv.Atoi(numLong.Value); err == nil {
			return n
		}
	}
	// Try plain number
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	return 0
}

// parseMongoDate extracts date string from MongoDB $date or plain string
func parseMongoDate(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// Try $date first
	var mongoDate MongoDate
	if err := json.Unmarshal(raw, &mongoDate); err == nil && mongoDate.Value != "" {
		return mongoDate.Value
	}
	// Try plain string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return ""
}

// parseMongoString extracts string from MongoDB $numberLong or plain string/number
func parseMongoString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// Try $numberLong first (MongoDB exports IDs as numbers sometimes)
	var numLong MongoNumberLong
	if err := json.Unmarshal(raw, &numLong); err == nil && numLong.Value != "" {
		return numLong.Value
	}
	// Try plain string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	// Try plain number
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return strconv.FormatInt(n, 10)
	}
	return ""
}

// convertMongoToOld converts MongoDB format to OldDataModel
func convertMongoToOld(m MongoDataModel) OldDataModel {
	errorStr := ""
	if m.Error != nil {
		errorStr = *m.Error
	}
	return OldDataModel{
		CtType:     parseMongoInt(m.CtType),
		DiskSize:   parseMongoInt(m.DiskSize),
		CoreCount:  parseMongoInt(m.CoreCount),
		RamSize:    parseMongoInt(m.RamSize),
		OsType:     m.OsType,
		OsVersion:  m.OsVersion,
		DisableIP6: m.DisableIP6,
		NsApp:      m.NsApp,
		Method:     m.Method,
		CreatedAt:  parseMongoDate(m.CreatedAt),
		PveVersion: m.PveVersion,
		Status:     m.Status,
		RandomID:   m.RandomID,
		Type:       m.Type,
		Error:      errorStr,
	}
}

// PBRecord represents the PocketBase record format
type PBRecord struct {
	CtType     int    `json:"ct_type"`
	DiskSize   int    `json:"disk_size"`
	CoreCount  int    `json:"core_count"`
	RamSize    int    `json:"ram_size"`
	OsType     string `json:"os_type"`
	OsVersion  string `json:"os_version"`
	DisableIP6 string `json:"disableip6"`
	NsApp      string `json:"nsapp"`
	Method     string `json:"method"`
	PveVersion string `json:"pve_version"`
	Status     string `json:"status"`
	RandomID   string `json:"random_id"`
	Type       string `json:"type"`
	Error      string `json:"error"`
	RepoSource string `json:"repo_source,omitempty"`
	OldCreated string `json:"old_created,omitempty"`
}

type Summary struct {
	TotalEntries int `json:"total_entries"`
}

type ImportJob struct {
	Record OldDataModel
	Index  int
}

type ImportResult struct {
	Success bool
	Skipped bool
	Error   error
}

func main() {
	// Setup source URLs
	baseURL := os.Getenv("MIGRATION_SOURCE_URL")
	if baseURL == "" {
		baseURL = defaultSourceAPI
	}
	sourceAPI = baseURL + "/paginated"
	summaryAPI = baseURL + "/summary"

	// Repo source (to distinguish data origins)
	repoSource = os.Getenv("REPO_SOURCE")

	// Date filters
	if dateStr := os.Getenv("DATE_UNTIL"); dateStr != "" {
		parsed, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			fmt.Printf("[ERROR] Invalid DATE_UNTIL format (use YYYY-MM-DD): %v\n", err)
			os.Exit(1)
		}
		dateUntil = parsed.Add(24*time.Hour - time.Second) // End of day
		hasDateUntil = true
	}

	if dateStr := os.Getenv("DATE_FROM"); dateStr != "" {
		parsed, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			fmt.Printf("[ERROR] Invalid DATE_FROM format (use YYYY-MM-DD): %v\n", err)
			os.Exit(1)
		}
		dateFrom = parsed // Start of day
		hasDateFrom = true
	}

	// Batch size
	batchSize := defaultBatchSize
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if n, err := strconv.Atoi(bs); err == nil && n > 0 {
			batchSize = n
		}
	}

	// Start page (for resuming)
	startPage := 1
	if sp := os.Getenv("START_PAGE"); sp != "" {
		if n, err := strconv.Atoi(sp); err == nil && n > 0 {
			startPage = n
		}
	}

	// PocketBase URL
	pbURL := os.Getenv("POCKETBASE_URL")
	if pbURL == "" {
		pbURL = os.Getenv("PB_URL")
	}
	if pbURL == "" {
		pbURL = defaultPBURL
	}

	// Collection
	pbCollection := os.Getenv("POCKETBASE_COLLECTION")
	if pbCollection == "" {
		pbCollection = os.Getenv("PB_TARGET_COLLECTION")
	}
	if pbCollection == "" {
		pbCollection = "telemetry"
	}

	// Auth collection
	authCollection := os.Getenv("PB_AUTH_COLLECTION")
	if authCollection == "" {
		authCollection = "telemetry_service_user"
	}

	// Credentials
	pbIdentity := os.Getenv("PB_IDENTITY")
	pbPassword := os.Getenv("PB_PASSWORD")

	// Workers count
	if wc := os.Getenv("WORKERS"); wc != "" {
		if n, err := strconv.Atoi(wc); err == nil && n > 0 {
			workerCount = n
		}
	}

	// Check for SQL output mode (fastest - no network!)
	jsonFile := os.Getenv("JSON_FILE")
	sqlOutput := os.Getenv("SQL_OUTPUT")
	if jsonFile != "" && sqlOutput != "" {
		runSQLExport(jsonFile, sqlOutput, pbCollection)
		return
	}

	// Check for JSON file import mode (via HTTP API)
	if jsonFile != "" {
		// Authenticate first
		if pbIdentity != "" && pbPassword != "" {
			fmt.Println("[AUTH] Authenticating with PocketBase...")
			err := authenticate(pbURL, authCollection, pbIdentity, pbPassword)
			if err != nil {
				fmt.Printf("[ERROR] Authentication failed: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("[OK] Authentication successful")
		}

		// Skip records (for resuming)
		var skipRecords int64
		if sr := os.Getenv("SKIP_RECORDS"); sr != "" {
			if n, err := strconv.ParseInt(sr, 10, 64); err == nil && n > 0 {
				skipRecords = n
			}
		}

		runJSONFileImport(jsonFile, pbURL, pbCollection, batchSize, skipRecords)
		return
	}

	fmt.Println("=========================================================")
	fmt.Println("        Data Migration to PocketBase")
	fmt.Println("=========================================================")
	fmt.Printf("Source API:       %s\n", baseURL)
	fmt.Printf("PocketBase URL:   %s\n", pbURL)
	fmt.Printf("Collection:       %s\n", pbCollection)
	fmt.Printf("Batch Size:       %d\n", batchSize)
	fmt.Printf("Workers:          %d\n", workerCount)
	if repoSource != "" {
		fmt.Printf("Repo Source:      %s\n", repoSource)
	}
	if hasDateFrom {
		fmt.Printf("Date From:        >= %s\n", dateFrom.Format("2006-01-02"))
	}
	if hasDateUntil {
		fmt.Printf("Date Until:       <= %s\n", dateUntil.Format("2006-01-02"))
	}
	if startPage > 1 {
		fmt.Printf("Starting Page:    %d\n", startPage)
	}
	fmt.Println("---------------------------------------------------------")

	// Authenticate with PocketBase
	if pbIdentity != "" && pbPassword != "" {
		fmt.Println("[AUTH] Authenticating with PocketBase...")
		err := authenticate(pbURL, authCollection, pbIdentity, pbPassword)
		if err != nil {
			fmt.Printf("[ERROR] Authentication failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[OK] Authentication successful")
	} else {
		fmt.Println("[WARN] No credentials provided, trying without auth...")
	}
	fmt.Println("---------------------------------------------------------")

	// Get total count
	summary, err := getSummary()
	if err != nil {
		fmt.Printf("[ERROR] Failed to get summary: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[INFO] Total entries in source: %d\n", summary.TotalEntries)
	fmt.Println("---------------------------------------------------------")

	// Calculate pages
	totalPages := (summary.TotalEntries + batchSize - 1) / batchSize

	fmt.Printf("Starting migration (%d pages to process)...\n", totalPages)
	fmt.Println()

	var totalMigrated, totalFailed, totalSkipped, totalFiltered int64
	startTime := time.Now()

	// Progress tracking
	processedRecords := int64(0)

	for page := startPage; page <= totalPages; page++ {
		pageStart := time.Now()

		data, err := fetchPage(page, batchSize)
		if err != nil {
			fmt.Printf("[Page %d] ERROR: Failed to fetch: %v\n", page, err)
			atomic.AddInt64(&totalFailed, int64(batchSize))
			continue
		}

		// Filter by date if needed
		var filteredData []OldDataModel
		var lastTimestamp string
		pageFiltered := 0

		for _, record := range data {
			recordDate, err := parseTimestamp(record.CreatedAt)
			if err != nil {
				// Skip records with unparseable dates
				atomic.AddInt64(&totalFiltered, 1)
				pageFiltered++
				continue
			}

			// Check DATE_FROM (skip if before)
			if hasDateFrom && recordDate.Before(dateFrom) {
				atomic.AddInt64(&totalFiltered, 1)
				pageFiltered++
				continue
			}

			// Check DATE_UNTIL (skip if after)
			if hasDateUntil && recordDate.After(dateUntil) {
				atomic.AddInt64(&totalFiltered, 1)
				pageFiltered++
				continue
			}

			filteredData = append(filteredData, record)
			lastTimestamp = record.CreatedAt
		}

		atomic.AddInt64(&processedRecords, int64(len(data)))

		// Show progress even if all filtered
		if len(filteredData) == 0 {
			if page <= 5 || page%100 == 0 {
				fmt.Printf("[Page %d/%d] All %d filtered (after DATE_UNTIL) | Total filtered: %d\n",
					page, totalPages, pageFiltered, atomic.LoadInt64(&totalFiltered))
			}
			continue
		}

		// Worker pool for importing
		jobs := make(chan ImportJob, len(filteredData))
		results := make(chan ImportResult, len(filteredData))
		var wg sync.WaitGroup

		// Start workers
		for w := 0; w < workerCount; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobs {
					result := importRecordWithRetry(pbURL, pbCollection, job.Record, 3)
					results <- result
				}
			}()
		}

		// Send jobs
		for i, record := range filteredData {
			jobs <- ImportJob{Record: record, Index: i}
		}
		close(jobs)

		// Wait for workers to finish
		go func() {
			wg.Wait()
			close(results)
		}()

		// Collect results
		pageMigrated, pageSkipped, pageFailed := 0, 0, 0
		var lastError error
		for result := range results {
			if result.Skipped {
				pageSkipped++
			} else if result.Success {
				pageMigrated++
			} else {
				pageFailed++
				if lastError == nil && result.Error != nil {
					lastError = result.Error
				}
			}
		}

		// Log first error on the page
		if pageFailed > 0 && lastError != nil && page <= 3 {
			fmt.Printf("[Page %d] Sample error: %v\n", page, lastError)
		}

		atomic.AddInt64(&totalMigrated, int64(pageMigrated))
		atomic.AddInt64(&totalSkipped, int64(pageSkipped))
		atomic.AddInt64(&totalFailed, int64(pageFailed))

		// Progress display (first 5 pages, then every 10 pages)
		if page <= 5 || page%10 == 0 || page == totalPages {
			elapsed := time.Since(startTime)
			processed := atomic.LoadInt64(&processedRecords)
			rate := float64(processed) / elapsed.Seconds()
			remaining := float64(summary.TotalEntries-int(processed)) / rate
			eta := time.Duration(remaining) * time.Second

			// Format last timestamp for display
			lastDate := ""
			if lastTimestamp != "" {
				if t, err := parseTimestamp(lastTimestamp); err == nil {
					lastDate = t.Format("2006-01-02")
				}
			}

			fmt.Printf("[Page %d/%d] Migrated: %d | Skipped: %d | Failed: %d | Filtered: %d | %.0f rec/s | Last: %s | ETA: %s\n",
				page, totalPages,
				atomic.LoadInt64(&totalMigrated),
				atomic.LoadInt64(&totalSkipped),
				atomic.LoadInt64(&totalFailed),
				atomic.LoadInt64(&totalFiltered),
				rate,
				lastDate,
				formatDuration(eta))
		}

		// Adaptive delay based on page processing time
		pageTime := time.Since(pageStart)
		if pageTime < 500*time.Millisecond {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("        Migration Complete")
	fmt.Println("=========================================================")
	fmt.Printf("Successfully migrated: %d\n", atomic.LoadInt64(&totalMigrated))
	fmt.Printf("Skipped (duplicates):  %d\n", atomic.LoadInt64(&totalSkipped))
	fmt.Printf("Filtered (date):       %d\n", atomic.LoadInt64(&totalFiltered))
	fmt.Printf("Failed:                %d\n", atomic.LoadInt64(&totalFailed))
	fmt.Printf("Duration:              %s\n", formatDuration(time.Since(startTime)))
	fmt.Println("=========================================================")

	if atomic.LoadInt64(&totalMigrated) > 0 {
		fmt.Println()
		fmt.Println("Next steps for timestamp migration:")
		fmt.Printf("   1. SSH into your PocketBase server\n")
		fmt.Printf("   2. Run: sqlite3 /app/pb_data/data.db \".tables\"\n")
		fmt.Printf("   3. Find your collection table name\n")
		fmt.Printf("   4. Run: sqlite3 /app/pb_data/data.db \"UPDATE <table_name> SET created = old_created, updated = old_created WHERE old_created IS NOT NULL AND old_created != ''\"\n")
		fmt.Printf("   5. Remove the old_created field from the collection in PocketBase Admin UI\n")
	}
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "calculating..."
	}
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func parseTimestamp(ts string) (time.Time, error) {
	if ts == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05.000 UTC",
		"2006-01-02T15:04:05.000+00:00",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, ts); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse: %s", ts)
}

func convertTimestamp(ts string) string {
	if ts == "" {
		return ""
	}

	t, err := parseTimestamp(ts)
	if err != nil {
		return ""
	}

	return t.UTC().Format("2006-01-02 15:04:05.000Z")
}

func getSummary() (*Summary, error) {
	resp, err := http.Get(summaryAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var summary Summary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nil, err
	}

	return &summary, nil
}

func authenticate(pbURL, authCollection, identity, password string) error {
	body := map[string]string{
		"identity": identity,
		"password": password,
	}
	jsonData, _ := json.Marshal(body)

	url := fmt.Sprintf("%s/api/collections/%s/auth-with-password", pbURL, authCollection)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if result.Token == "" {
		return fmt.Errorf("no token in response")
	}

	authToken = result.Token
	return nil
}

func fetchPage(page, limit int) ([]OldDataModel, error) {
	url := fmt.Sprintf("%s?page=%d&limit=%d", sourceAPI, page, limit)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var data []OldDataModel
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

func importRecordWithRetry(pbURL, collection string, old OldDataModel, maxRetries int) ImportResult {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := importRecord(pbURL, collection, old)
		if err == nil {
			return ImportResult{Success: true}
		}
		if isUniqueViolation(err) {
			return ImportResult{Skipped: true}
		}
		lastErr = err
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	return ImportResult{Error: lastErr}
}

func importRecord(pbURL, collection string, old OldDataModel) error {
	// Map status: "done" -> "success"
	status := old.Status
	switch status {
	case "done":
		status = "success"
	case "installing", "failed", "unknown", "success":
		// keep as-is
	default:
		status = "unknown"
	}

	// Map ct_type: 1=unprivileged (0), 2=privileged (1)
	// PocketBase schema: 0 = unprivileged, 1 = privileged
	ctType := 0 // default: unprivileged
	if old.CtType == 2 {
		ctType = 1 // privileged
	}

	// Ensure type is set
	recordType := old.Type
	if recordType == "" {
		recordType = "lxc"
	}

	// Ensure nsapp is set (required field)
	nsapp := old.NsApp
	if nsapp == "" {
		nsapp = "unknown"
	}

	record := PBRecord{
		CtType:     ctType,
		DiskSize:   old.DiskSize,
		CoreCount:  old.CoreCount,
		RamSize:    old.RamSize,
		OsType:     old.OsType,
		OsVersion:  old.OsVersion,
		DisableIP6: old.DisableIP6,
		NsApp:      nsapp,
		Method:     old.Method,
		PveVersion: old.PveVersion,
		Status:     status,
		RandomID:   old.RandomID,
		Type:       recordType,
		Error:      old.Error,
		RepoSource: repoSource,
		OldCreated: convertTimestamp(old.CreatedAt),
	}

	jsonData, err := json.Marshal(record)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/collections/%s/records", pbURL, collection)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "UNIQUE constraint failed") ||
		strings.Contains(errStr, "duplicate") ||
		strings.Contains(errStr, "already exists") ||
		strings.Contains(errStr, "validation_not_unique")
}

// runJSONFileImport handles importing from a JSON file using streaming
func runJSONFileImport(jsonFile, pbURL, pbCollection string, batchSize int, skipRecords int64) {
	fmt.Println("=========================================================")
	fmt.Println("        JSON File Import to PocketBase")
	fmt.Println("=========================================================")
	fmt.Printf("JSON File:        %s\n", jsonFile)
	fmt.Printf("PocketBase URL:   %s\n", pbURL)
	fmt.Printf("Collection:       %s\n", pbCollection)
	fmt.Printf("Batch Size:       %d\n", batchSize)
	fmt.Printf("Workers:          %d\n", workerCount)
	if repoSource != "" {
		fmt.Printf("Repo Source:      %s\n", repoSource)
	}
	if hasDateFrom {
		fmt.Printf("Date From:        >= %s\n", dateFrom.Format("2006-01-02"))
	}
	if hasDateUntil {
		fmt.Printf("Date Until:       <= %s\n", dateUntil.Format("2006-01-02"))
	}
	if skipRecords > 0 {
		fmt.Printf("Skip Records:     %d\n", skipRecords)
	}
	fmt.Println("---------------------------------------------------------")

	// Open file
	file, err := os.Open(jsonFile)
	if err != nil {
		fmt.Printf("[ERROR] Cannot open file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Get file size for progress
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	fmt.Printf("[INFO] File size: %.2f GB\n", float64(fileSize)/(1024*1024*1024))

	// Auto-detect MongoDB Extended JSON format by peeking at the first 2KB
	peekBuf := make([]byte, 2048)
	n, _ := file.Read(peekBuf)
	peekStr := string(peekBuf[:n])
	isMongoFormat := strings.Contains(peekStr, `"$oid"`) ||
		strings.Contains(peekStr, `"$numberLong"`) ||
		strings.Contains(peekStr, `"$date"`)

	// Reset file to beginning
	file.Seek(0, 0)

	if isMongoFormat {
		fmt.Println("[INFO] Detected MongoDB Extended JSON format")
	} else {
		fmt.Println("[INFO] Detected standard JSON format")
	}

	// Use buffered reader for better performance
	reader := bufio.NewReaderSize(file, 64*1024*1024) // 64MB buffer

	// Create JSON decoder for streaming
	decoder := json.NewDecoder(reader)

	// Expect array start
	token, err := decoder.Token()
	if err != nil {
		fmt.Printf("[ERROR] Cannot read JSON: %v\n", err)
		os.Exit(1)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		fmt.Printf("[ERROR] JSON file must be an array (expected '[', got %v)\n", token)
		os.Exit(1)
	}

	fmt.Println("[OK] JSON array detected, starting streaming import...")
	fmt.Println("---------------------------------------------------------")

	var totalMigrated, totalFailed, totalSkipped, totalFiltered int64
	var recordCount int64
	startTime := time.Now()

	// Batch processing
	batch := make([]OldDataModel, 0, batchSize)
	batchNum := 0

	for decoder.More() {
		var record OldDataModel

		if isMongoFormat {
			// Decode as MongoDB Extended JSON
			var mongoRecord MongoDataModel
			if err := decoder.Decode(&mongoRecord); err != nil {
				fmt.Printf("[WARN] Failed to decode record %d: %v\n", recordCount+1, err)
				atomic.AddInt64(&totalFailed, 1)
				recordCount++
				continue
			}
			record = convertMongoToOld(mongoRecord)
		} else {
			// Decode as standard JSON
			if err := decoder.Decode(&record); err != nil {
				fmt.Printf("[WARN] Failed to decode record %d: %v\n", recordCount+1, err)
				atomic.AddInt64(&totalFailed, 1)
				recordCount++
				continue
			}
		}
		recordCount++

		// Skip records (for resuming)
		if recordCount <= skipRecords {
			if recordCount%100000 == 0 {
				fmt.Printf("[SKIP] Skipped %d / %d records...\n", recordCount, skipRecords)
			}
			continue
		}

		// Date filter
		if hasDateFrom || hasDateUntil {
			recordDate, err := parseTimestamp(record.CreatedAt)
			if err != nil {
				atomic.AddInt64(&totalFiltered, 1)
				continue
			}
			if hasDateFrom && recordDate.Before(dateFrom) {
				atomic.AddInt64(&totalFiltered, 1)
				continue
			}
			if hasDateUntil && recordDate.After(dateUntil) {
				atomic.AddInt64(&totalFiltered, 1)
				continue
			}
		}

		batch = append(batch, record)

		// Process batch when full
		if len(batch) >= batchSize {
			batchNum++
			migrated, skipped, failed := processBatch(pbURL, pbCollection, batch, batchNum, recordCount, startTime)
			atomic.AddInt64(&totalMigrated, int64(migrated))
			atomic.AddInt64(&totalSkipped, int64(skipped))
			atomic.AddInt64(&totalFailed, int64(failed))
			batch = batch[:0] // Reset batch
		}
	}

	// Process remaining batch
	if len(batch) > 0 {
		batchNum++
		migrated, skipped, failed := processBatch(pbURL, pbCollection, batch, batchNum, recordCount, startTime)
		atomic.AddInt64(&totalMigrated, int64(migrated))
		atomic.AddInt64(&totalSkipped, int64(skipped))
		atomic.AddInt64(&totalFailed, int64(failed))
	}

	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("        JSON File Import Complete")
	fmt.Println("=========================================================")
	fmt.Printf("Total records read:    %d\n", recordCount)
	fmt.Printf("Successfully imported: %d\n", atomic.LoadInt64(&totalMigrated))
	fmt.Printf("Skipped (duplicates):  %d\n", atomic.LoadInt64(&totalSkipped))
	fmt.Printf("Filtered (date):       %d\n", atomic.LoadInt64(&totalFiltered))
	fmt.Printf("Failed:                %d\n", atomic.LoadInt64(&totalFailed))
	fmt.Printf("Duration:              %s\n", formatDuration(time.Since(startTime)))
	fmt.Println("=========================================================")

	if atomic.LoadInt64(&totalMigrated) > 0 {
		fmt.Println()
		fmt.Println("Next steps for timestamp migration:")
		fmt.Printf("   1. SSH into your PocketBase server\n")
		fmt.Printf("   2. Run: sqlite3 /app/pb_data/data.db \".tables\"\n")
		fmt.Printf("   3. Find your collection table name\n")
		fmt.Printf("   4. Run: sqlite3 /app/pb_data/data.db \"UPDATE <table_name> SET created = old_created, updated = old_created WHERE old_created IS NOT NULL AND old_created != ''\"\n")
		fmt.Printf("   5. Remove the old_created field from the collection in PocketBase Admin UI\n")
	}
}

func processBatch(pbURL, pbCollection string, records []OldDataModel, batchNum int, totalRead int64, startTime time.Time) (migrated, skipped, failed int) {
	batchStart := time.Now()

	// Worker pool
	jobs := make(chan ImportJob, len(records))
	results := make(chan ImportResult, len(records))
	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				result := importRecordWithRetry(pbURL, pbCollection, job.Record, 3)
				results <- result
			}
		}()
	}

	// Send jobs
	for i, record := range records {
		jobs <- ImportJob{Record: record, Index: i}
	}
	close(jobs)

	// Wait and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var firstError error
	for result := range results {
		if result.Skipped {
			skipped++
		} else if result.Success {
			migrated++
		} else {
			failed++
			if firstError == nil && result.Error != nil {
				firstError = result.Error
			}
		}
	}

	// Progress display
	elapsed := time.Since(startTime)
	rate := float64(totalRead) / elapsed.Seconds()

	var lastDate string
	if len(records) > 0 {
		if t, err := parseTimestamp(records[len(records)-1].CreatedAt); err == nil {
			lastDate = t.Format("2006-01-02")
		}
	}

	fmt.Printf("[Batch %d] Read: %d | Migrated: %d | Skipped: %d | Failed: %d | %.0f rec/s | Last: %s | Batch: %v\n",
		batchNum, totalRead, migrated, skipped, failed, rate, lastDate, time.Since(batchStart).Round(time.Millisecond))

	// Show first error for debugging
	if firstError != nil && batchNum <= 3 {
		fmt.Printf("         [ERROR] Sample error: %v\n", firstError)
	}

	return migrated, skipped, failed
}

// runSQLExport generates a SQL file for direct SQLite import (fastest method!)
func runSQLExport(jsonFile, sqlOutput, tableName string) {
	fmt.Println("=========================================================")
	fmt.Println("        SQL Export Mode (Direct SQLite Import)")
	fmt.Println("=========================================================")
	fmt.Printf("JSON File:   %s\n", jsonFile)
	fmt.Printf("SQL Output:  %s\n", sqlOutput)
	fmt.Printf("Table:       %s\n", tableName)
	fmt.Printf("Repo Source: %s\n", repoSource)
	fmt.Println("---------------------------------------------------------")

	// Open JSON file
	file, err := os.Open(jsonFile)
	if err != nil {
		fmt.Printf("[ERROR] Cannot open JSON file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Get file size for progress
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	fmt.Printf("[INFO] File size: %.2f GB\n", float64(fileSize)/(1024*1024*1024))

	// Open SQL output file stream
	sqlFile, err := os.Create(sqlOutput)
	if err != nil {
		fmt.Printf("[ERROR] Cannot create SQL file: %v\n", err)
		os.Exit(1)
	}
	defer sqlFile.Close()

	writer := bufio.NewWriterSize(sqlFile, 1*1024*1024) // 1MB buffer for faster feedback

	// Write SQL header
	writer.WriteString("-- Auto-generated SQL import for PocketBase\n")
	writer.WriteString("-- Generated: " + time.Now().Format(time.RFC3339) + "\n")
	writer.WriteString("PRAGMA journal_mode=WAL;\n")
	writer.WriteString("PRAGMA synchronous=OFF;\n")
	writer.WriteString("PRAGMA cache_size=100000;\n")
	writer.WriteString("BEGIN TRANSACTION;\n\n")
	writer.Flush() // Flush header immediately
	fmt.Println("[INFO] SQL header written, starting record processing...")

	startTime := time.Now()
	var recordCount int64
	var filteredCount int64

	// Create buffered reader
	reader := bufio.NewReaderSize(file, 16*1024*1024)

	// Skip BOM and whitespace, find the opening '[' 
	fmt.Println("[INFO] Scanning for JSON array start...")
	for {
		b, err := reader.ReadByte()
		if err != nil {
			fmt.Printf("[ERROR] Cannot read JSON file: %v\n", err)
			os.Exit(1)
		}
		// Skip UTF-8 BOM (EF BB BF) and whitespace
		if b == 0xEF || b == 0xBB || b == 0xBF || b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if b == '[' {
			fmt.Println("[INFO] Found JSON array, starting decode...")
			break // Found the start of array
		}
		fmt.Printf("[ERROR] Invalid JSON array format (expected '[', got 0x%02X '%c')\n", b, b)
		os.Exit(1)
	}

	decoder := json.NewDecoder(reader)

	for {
		// Check for end of array
		if !decoder.More() {
			break
		}

		var mongoRecord MongoDataModel
		if err := decoder.Decode(&mongoRecord); err != nil {
			if err == io.EOF {
				break
			}
			// Log first few errors for debugging
			if recordCount < 5 {
				fmt.Printf("[WARN] Skipping malformed record: %v\n", err)
			}
			continue
		}

		// Convert MongoDB format to normal format
		record := OldDataModel{
			ID:         parseMongoString(mongoRecord.ID),
			CtType:     parseMongoInt(mongoRecord.CtType),
			DiskSize:   parseMongoInt(mongoRecord.DiskSize),
			CoreCount:  parseMongoInt(mongoRecord.CoreCount),
			RamSize:    parseMongoInt(mongoRecord.RamSize),
			OsType:     mongoRecord.OsType,
			OsVersion:  mongoRecord.OsVersion,
			DisableIP6: mongoRecord.DisableIP6,
			NsApp:      mongoRecord.NsApp,
			Method:     mongoRecord.Method,
			CreatedAt:  parseMongoDate(mongoRecord.CreatedAt),
			PveVersion: mongoRecord.PveVersion,
			Status:     mongoRecord.Status,
			RandomID:   mongoRecord.RandomID,
			Type:       mongoRecord.Type,
		}
		if mongoRecord.Error != nil {
			record.Error = *mongoRecord.Error
		}

		// Apply date filters
		if hasDateFrom || hasDateUntil {
			recordTime, err := parseTimestamp(record.CreatedAt)
			if err == nil {
				if hasDateFrom && recordTime.Before(dateFrom) {
					filteredCount++
					continue
				}
				if hasDateUntil && recordTime.After(dateUntil) {
					filteredCount++
					continue
				}
			}
		}

		// Generate unique ID for PocketBase
		pbID := generatePocketBaseID()

		// Normalize values
		status := record.Status
		if status == "done" {
			status = "success"
		}
		if status == "" {
			status = "unknown"
		}

		nsapp := record.NsApp
		if nsapp == "" {
			nsapp = "unknown"
		}

		disableip6 := record.DisableIP6
		if disableip6 == "" {
			disableip6 = "no"
		}

		recType := record.Type
		if recType == "" {
			recType = "lxc"
		}

		// Format created date
		createdAt := record.CreatedAt
		if createdAt == "" {
			createdAt = time.Now().UTC().Format("2006-01-02 15:04:05.000Z")
		}

		// Escape strings for SQL
		escapeSQLString := func(s string) string {
			return strings.ReplaceAll(s, "'", "''")
		}

		// Write INSERT statement
		sql := fmt.Sprintf(
			"INSERT OR IGNORE INTO %s (id,created,updated,ct_type,disk_size,core_count,ram_size,os_type,os_version,disableip6,nsapp,method,pve_version,status,random_id,type,error,repo_source) VALUES ('%s','%s','%s',%d,%d,%d,%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');\n",
			tableName,
			pbID,
			escapeSQLString(createdAt),
			escapeSQLString(createdAt),
			record.CtType,
			record.DiskSize,
			record.CoreCount,
			record.RamSize,
			escapeSQLString(record.OsType),
			escapeSQLString(record.OsVersion),
			escapeSQLString(disableip6),
			escapeSQLString(nsapp),
			escapeSQLString(record.Method),
			escapeSQLString(record.PveVersion),
			escapeSQLString(status),
			escapeSQLString(record.RandomID),
			escapeSQLString(recType),
			escapeSQLString(record.Error),
			escapeSQLString(repoSource),
		)

		writer.WriteString(sql)
		recordCount++

		// Progress every 10k records (and flush to show file growing)
		if recordCount%10000 == 0 {
			writer.Flush()
			elapsed := time.Since(startTime)
			rate := float64(recordCount) / elapsed.Seconds()
			fmt.Printf("[PROGRESS] %d records processed (%.0f rec/s)\n", recordCount, rate)
		}
	}

	// Write footer
	writer.WriteString("\nCOMMIT;\n")
	writer.Flush()

	elapsed := time.Since(startTime)
	rate := float64(recordCount) / elapsed.Seconds()

	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("        SQL Export Complete")
	fmt.Println("=========================================================")
	fmt.Printf("Records exported:    %d\n", recordCount)
	fmt.Printf("Filtered (date):     %d\n", filteredCount)
	fmt.Printf("Duration:            %s\n", formatDuration(elapsed))
	fmt.Printf("Speed:               %.0f records/sec\n", rate)
	fmt.Printf("Output file:         %s\n", sqlOutput)
	fmt.Println("---------------------------------------------------------")
	fmt.Println()
	fmt.Println("To import into PocketBase:")
	fmt.Printf("  sqlite3 /app/pb_data/data.db < %s\n", sqlOutput)
	fmt.Println()
}

// generatePocketBaseID generates a 15-char alphanumeric ID like PocketBase does
func generatePocketBaseID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 15)
	// Use crypto/rand for randomness
	randBytes := make([]byte, 15)
	if _, err := io.ReadFull(cryptoRand.Reader, randBytes); err != nil {
		// Fallback to time-based
		for i := range b {
			b[i] = chars[(time.Now().UnixNano()+int64(i))%int64(len(chars))]
		}
		return string(b)
	}
	for i := range b {
		b[i] = chars[randBytes[i]%byte(len(chars))]
	}
	return string(b)
}
