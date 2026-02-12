package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

// CleanupConfig holds configuration for the cleanup job
type CleanupConfig struct {
	Enabled          bool
	CheckInterval    time.Duration // How often to run cleanup
	StuckAfterHours  int           // Consider "installing" as stuck after X hours
	RetentionDays    int           // Delete records older than X days (0 = disabled)
	RetentionEnabled bool          // Enable automatic data retention/deletion
}

// Cleaner handles cleanup of stuck installations
type Cleaner struct {
	cfg CleanupConfig
	pb  *PBClient
}

// NewCleaner creates a new cleaner instance
func NewCleaner(cfg CleanupConfig, pb *PBClient) *Cleaner {
	return &Cleaner{
		cfg: cfg,
		pb:  pb,
	}
}

// Start begins the cleanup loop
func (c *Cleaner) Start() {
	if !c.cfg.Enabled {
		log.Println("INFO: cleanup job disabled")
		return
	}

	go c.cleanupLoop()
	log.Printf("INFO: cleanup job started (interval: %v, stuck after: %d hours)", c.cfg.CheckInterval, c.cfg.StuckAfterHours)
	
	// Start retention job if enabled
	if c.cfg.RetentionEnabled && c.cfg.RetentionDays > 0 {
		go c.retentionLoop()
		log.Printf("INFO: data retention job started (delete after: %d days)", c.cfg.RetentionDays)
	}
}

func (c *Cleaner) cleanupLoop() {
	// Run immediately on start
	c.runCleanup()

	ticker := time.NewTicker(c.cfg.CheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.runCleanup()
	}
}

// runCleanup finds and updates stuck installations
func (c *Cleaner) runCleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Find stuck records
	stuckRecords, err := c.findStuckInstallations(ctx)
	if err != nil {
		log.Printf("WARN: cleanup - failed to find stuck installations: %v", err)
		return
	}

	if len(stuckRecords) == 0 {
		log.Printf("INFO: cleanup - no stuck installations found")
		return
	}

	log.Printf("INFO: cleanup - found %d stuck installations", len(stuckRecords))

	// Update each record
	updated := 0
	for _, record := range stuckRecords {
		if err := c.markAsUnknown(ctx, record.ID); err != nil {
			log.Printf("WARN: cleanup - failed to update record %s: %v", record.ID, err)
			continue
		}
		updated++
	}

	log.Printf("INFO: cleanup - updated %d stuck installations to 'unknown'", updated)
}

// StuckRecord represents a minimal record for cleanup
type StuckRecord struct {
	ID      string `json:"id"`
	NSAPP   string `json:"nsapp"`
	Created string `json:"created"`
}

// findStuckInstallations finds records that are stuck in "installing" status
func (c *Cleaner) findStuckInstallations(ctx context.Context) ([]StuckRecord, error) {
	if err := c.pb.ensureAuth(ctx); err != nil {
		return nil, err
	}

	// Calculate cutoff time
	cutoff := time.Now().Add(-time.Duration(c.cfg.StuckAfterHours) * time.Hour)
	cutoffStr := cutoff.Format("2006-01-02 15:04:05")

	// Build filter: status='installing' AND created < cutoff
	filter := url.QueryEscape(fmt.Sprintf("status='installing' && created<'%s'", cutoffStr))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/collections/%s/records?filter=%s&perPage=100",
			c.pb.baseURL, c.pb.targetColl, filter),
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.pb.token)

	resp, err := c.pb.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Items []StuckRecord `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Items, nil
}

// markAsUnknown updates a record's status to "unknown"
func (c *Cleaner) markAsUnknown(ctx context.Context, recordID string) error {
	update := TelemetryStatusUpdate{
		Status: "unknown",
		Error:  "Installation timed out - no completion status received",
	}
	return c.pb.UpdateTelemetryStatus(ctx, recordID, update)
}

// RunNow triggers an immediate cleanup run (for testing/manual trigger)
func (c *Cleaner) RunNow() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stuckRecords, err := c.findStuckInstallations(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to find stuck installations: %w", err)
	}

	updated := 0
	for _, record := range stuckRecords {
		if err := c.markAsUnknown(ctx, record.ID); err != nil {
			log.Printf("WARN: cleanup - failed to update record %s: %v", record.ID, err)
			continue
		}
		updated++
	}

	return updated, nil
}

// GetStuckCount returns the current number of stuck installations
func (c *Cleaner) GetStuckCount(ctx context.Context) (int, error) {
	records, err := c.findStuckInstallations(ctx)
	if err != nil {
		return 0, err
	}
	return len(records), nil
}

// =============================================
// DATA RETENTION (GDPR Löschkonzept)
// =============================================

// retentionLoop runs the data retention job periodically (once per day)
func (c *Cleaner) retentionLoop() {
	// Run once on startup after a delay
	time.Sleep(5 * time.Minute)
	c.runRetention()

	// Run daily at 3:00 AM
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		c.runRetention()
	}
}

// runRetention deletes records older than RetentionDays
func (c *Cleaner) runRetention() {
	if c.cfg.RetentionDays <= 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log.Printf("INFO: retention - starting cleanup of records older than %d days", c.cfg.RetentionDays)

	deleted, err := c.deleteOldRecords(ctx)
	if err != nil {
		log.Printf("WARN: retention - failed to delete old records: %v", err)
		return
	}

	if deleted > 0 {
		log.Printf("INFO: retention - deleted %d records older than %d days", deleted, c.cfg.RetentionDays)
	} else {
		log.Printf("INFO: retention - no records to delete")
	}
}

// deleteOldRecords finds and deletes records older than RetentionDays
func (c *Cleaner) deleteOldRecords(ctx context.Context) (int, error) {
	if err := c.pb.ensureAuth(ctx); err != nil {
		return 0, err
	}

	// Calculate cutoff date
	cutoff := time.Now().AddDate(0, 0, -c.cfg.RetentionDays)
	cutoffStr := cutoff.Format("2006-01-02 00:00:00")

	deleted := 0
	page := 1
	maxDeletePerRun := 1000 // Limit to prevent timeout

	for deleted < maxDeletePerRun {
		// Find old records
		filter := url.QueryEscape(fmt.Sprintf("created<'%s'", cutoffStr))
		reqURL := fmt.Sprintf("%s/api/collections/%s/records?filter=%s&perPage=100&page=%d",
			c.pb.baseURL, c.pb.targetColl, filter, page)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return deleted, err
		}
		req.Header.Set("Authorization", "Bearer "+c.pb.token)

		resp, err := c.pb.http.Do(req)
		if err != nil {
			return deleted, err
		}

		var result struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
			TotalItems int `json:"totalItems"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return deleted, err
		}
		resp.Body.Close()

		if len(result.Items) == 0 {
			break
		}

		// Delete each record
		for _, item := range result.Items {
			if err := c.deleteRecord(ctx, item.ID); err != nil {
				log.Printf("WARN: retention - failed to delete record %s: %v", item.ID, err)
				continue
			}
			deleted++

			if deleted >= maxDeletePerRun {
				log.Printf("INFO: retention - reached max delete limit (%d), will continue next run", maxDeletePerRun)
				return deleted, nil
			}
		}

		// Don't increment page since we deleted records
	}

	return deleted, nil
}

// deleteRecord permanently deletes a record from PocketBase
func (c *Cleaner) deleteRecord(ctx context.Context, recordID string) error {
	reqURL := fmt.Sprintf("%s/api/collections/%s/records/%s",
		c.pb.baseURL, c.pb.targetColl, recordID)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.pb.token)

	resp, err := c.pb.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetRetentionStats returns statistics about records eligible for deletion
func (c *Cleaner) GetRetentionStats(ctx context.Context) (eligible int, oldestDate string, err error) {
	if c.cfg.RetentionDays <= 0 {
		return 0, "", nil
	}

	if err := c.pb.ensureAuth(ctx); err != nil {
		return 0, "", err
	}

	cutoff := time.Now().AddDate(0, 0, -c.cfg.RetentionDays)
	cutoffStr := cutoff.Format("2006-01-02 00:00:00")
	filter := url.QueryEscape(fmt.Sprintf("created<'%s'", cutoffStr))

	reqURL := fmt.Sprintf("%s/api/collections/%s/records?filter=%s&perPage=1&sort=created",
		c.pb.baseURL, c.pb.targetColl, filter)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.pb.token)

	resp, err := c.pb.http.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	var result struct {
		Items []struct {
			Created string `json:"created"`
		} `json:"items"`
		TotalItems int `json:"totalItems"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, "", err
	}

	if len(result.Items) > 0 {
		oldestDate = result.Items[0].Created[:10] // Just the date part
	}

	return result.TotalItems, oldestDate, nil
}
