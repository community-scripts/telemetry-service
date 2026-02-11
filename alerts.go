package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/smtp"
	"strings"
	"sync"
	"time"
)

// AlertConfig holds SMTP alert configuration
type AlertConfig struct {
	Enabled          bool
	SMTPHost         string
	SMTPPort         int
	SMTPUser         string
	SMTPPassword     string
	SMTPFrom         string
	SMTPTo           []string
	UseTLS           bool
	FailureThreshold float64       // Alert when failure rate exceeds this (e.g., 20.0 = 20%)
	CheckInterval    time.Duration // How often to check
	Cooldown         time.Duration // Minimum time between alerts

	// Weekly Report settings
	WeeklyReportEnabled bool          // Enable weekly summary reports
	WeeklyReportDay     time.Weekday  // Day to send report (0=Sunday, 1=Monday, etc.)
	WeeklyReportHour    int           // Hour to send report (0-23)
}

// WeeklyReportData contains aggregated weekly statistics
type WeeklyReportData struct {
	CalendarWeek     int
	Year             int
	StartDate        time.Time
	EndDate          time.Time
	TotalInstalls    int
	SuccessCount     int
	FailedCount      int
	SuccessRate      float64
	TopApps          []AppStat
	TopFailedApps    []AppStat
	ComparedToPrev   WeekComparison
	OsDistribution   map[string]int
	TypeDistribution map[string]int
}

// AppStat represents statistics for a single app
type AppStat struct {
	Name        string
	Total       int
	Failed      int
	FailureRate float64
}

// WeekComparison shows changes compared to previous week
type WeekComparison struct {
	InstallsChange   int     // Difference in total installs
	InstallsPercent  float64 // Percentage change
	FailRateChange   float64 // Change in failure rate (percentage points)
}

// Alerter handles alerting functionality
type Alerter struct {
	cfg              AlertConfig
	lastAlertAt      time.Time
	lastWeeklyReport time.Time
	mu               sync.Mutex
	pb               *PBClient
	lastStats        alertStats
	alertHistory     []AlertEvent
}

type alertStats struct {
	successCount int
	failedCount  int
	checkedAt    time.Time
}

// AlertEvent records an alert that was sent
type AlertEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	Type        string    `json:"type"`
	Message     string    `json:"message"`
	FailureRate float64   `json:"failure_rate,omitempty"`
}

// NewAlerter creates a new alerter instance
func NewAlerter(cfg AlertConfig, pb *PBClient) *Alerter {
	return &Alerter{
		cfg:          cfg,
		pb:           pb,
		alertHistory: make([]AlertEvent, 0),
	}
}

// Start begins the alert monitoring loop
func (a *Alerter) Start() {
	if !a.cfg.Enabled {
		log.Println("INFO: alerting disabled")
		return
	}

	if a.cfg.SMTPHost == "" || len(a.cfg.SMTPTo) == 0 {
		log.Println("WARN: alerting enabled but SMTP not configured")
		return
	}

	go a.monitorLoop()
	log.Printf("INFO: alert monitoring started (threshold: %.1f%%, interval: %v)", a.cfg.FailureThreshold, a.cfg.CheckInterval)

	// Start weekly report scheduler if enabled
	if a.cfg.WeeklyReportEnabled {
		go a.weeklyReportLoop()
		log.Printf("INFO: weekly report scheduler started (day: %s, hour: %02d:00)", a.cfg.WeeklyReportDay, a.cfg.WeeklyReportHour)
	}
}

func (a *Alerter) monitorLoop() {
	ticker := time.NewTicker(a.cfg.CheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		a.checkAndAlert()
	}
}

func (a *Alerter) checkAndAlert() {
	ctx, cancel := newTimeoutContext(10 * time.Second)
	defer cancel()

	// Fetch last hour's data
	data, err := a.pb.FetchDashboardData(ctx, 1)
	if err != nil {
		log.Printf("WARN: alert check failed: %v", err)
		return
	}

	// Calculate current failure rate
	total := data.SuccessCount + data.FailedCount
	if total < 10 {
		// Not enough data to determine rate
		return
	}

	failureRate := float64(data.FailedCount) / float64(total) * 100

	// Check if we should alert
	if failureRate >= a.cfg.FailureThreshold {
		a.maybeSendAlert(failureRate, data.FailedCount, total)
	}
}

func (a *Alerter) maybeSendAlert(rate float64, failed, total int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check cooldown
	if time.Since(a.lastAlertAt) < a.cfg.Cooldown {
		return
	}

	// Send alert
	subject := fmt.Sprintf("[ProxmoxVED Alert] High Failure Rate: %.1f%%", rate)
	body := fmt.Sprintf(`ProxmoxVE Helper Scripts - Telemetry Alert

⚠️ High installation failure rate detected!

Current Statistics (last 24h):
- Failure Rate: %.1f%%
- Failed Installations: %d
- Total Installations: %d
- Threshold: %.1f%%

Time: %s

Please check the dashboard for more details.

---
This is an automated alert from the telemetry service.
`, rate, failed, total, a.cfg.FailureThreshold, time.Now().Format(time.RFC1123))

	if err := a.sendEmail(subject, body); err != nil {
		log.Printf("ERROR: failed to send alert email: %v", err)
		return
	}

	a.lastAlertAt = time.Now()
	a.alertHistory = append(a.alertHistory, AlertEvent{
		Timestamp:   time.Now(),
		Type:        "high_failure_rate",
		Message:     fmt.Sprintf("Failure rate %.1f%% exceeded threshold %.1f%%", rate, a.cfg.FailureThreshold),
		FailureRate: rate,
	})

	// Keep only last 100 alerts
	if len(a.alertHistory) > 100 {
		a.alertHistory = a.alertHistory[len(a.alertHistory)-100:]
	}

	log.Printf("ALERT: sent high failure rate alert (%.1f%%)", rate)
}

func (a *Alerter) sendEmail(subject, body string) error {
	// Build message
	var msg bytes.Buffer
	msg.WriteString(fmt.Sprintf("From: %s\r\n", a.cfg.SMTPFrom))
	msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(a.cfg.SMTPTo, ", ")))
	msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subject))
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(body)

	addr := fmt.Sprintf("%s:%d", a.cfg.SMTPHost, a.cfg.SMTPPort)

	var auth smtp.Auth
	if a.cfg.SMTPUser != "" && a.cfg.SMTPPassword != "" {
		auth = smtp.PlainAuth("", a.cfg.SMTPUser, a.cfg.SMTPPassword, a.cfg.SMTPHost)
	}

	if a.cfg.UseTLS {
		// TLS connection
		tlsConfig := &tls.Config{
			ServerName: a.cfg.SMTPHost,
		}

		conn, err := tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return fmt.Errorf("TLS dial failed: %w", err)
		}
		defer conn.Close()

		client, err := smtp.NewClient(conn, a.cfg.SMTPHost)
		if err != nil {
			return fmt.Errorf("SMTP client failed: %w", err)
		}
		defer client.Close()

		if auth != nil {
			if err := client.Auth(auth); err != nil {
				return fmt.Errorf("SMTP auth failed: %w", err)
			}
		}

		if err := client.Mail(a.cfg.SMTPFrom); err != nil {
			return fmt.Errorf("SMTP MAIL failed: %w", err)
		}

		for _, to := range a.cfg.SMTPTo {
			if err := client.Rcpt(to); err != nil {
				return fmt.Errorf("SMTP RCPT failed: %w", err)
			}
		}

		w, err := client.Data()
		if err != nil {
			return fmt.Errorf("SMTP DATA failed: %w", err)
		}

		_, err = w.Write(msg.Bytes())
		if err != nil {
			return fmt.Errorf("SMTP write failed: %w", err)
		}

		return w.Close()
	}

	// Non-TLS (STARTTLS)
	return smtp.SendMail(addr, auth, a.cfg.SMTPFrom, a.cfg.SMTPTo, msg.Bytes())
}

// GetAlertHistory returns recent alert events
func (a *Alerter) GetAlertHistory() []AlertEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := make([]AlertEvent, len(a.alertHistory))
	copy(result, a.alertHistory)
	return result
}

// TestAlert sends a test alert email
func (a *Alerter) TestAlert() error {
	if !a.cfg.Enabled || a.cfg.SMTPHost == "" {
		return fmt.Errorf("alerting not configured")
	}

	subject := "[ProxmoxVED] Test Alert"
	body := fmt.Sprintf(`This is a test alert from ProxmoxVE Helper Scripts telemetry service.

If you received this email, your alert configuration is working correctly.

Time: %s
SMTP Host: %s
Recipients: %s

---
This is an automated test message.
`, time.Now().Format(time.RFC1123), a.cfg.SMTPHost, strings.Join(a.cfg.SMTPTo, ", "))

	return a.sendEmail(subject, body)
}

// Helper for timeout context
func newTimeoutContext(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// weeklyReportLoop checks periodically if it's time to send the weekly report
func (a *Alerter) weeklyReportLoop() {
	// Check every hour
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		a.checkAndSendWeeklyReport()
	}
}

// checkAndSendWeeklyReport sends the weekly report if it's the right time
func (a *Alerter) checkAndSendWeeklyReport() {
	now := time.Now()

	// Check if it's the right day and hour
	if now.Weekday() != a.cfg.WeeklyReportDay || now.Hour() != a.cfg.WeeklyReportHour {
		return
	}

	a.mu.Lock()
	// Check if we already sent a report this week
	_, lastWeek := a.lastWeeklyReport.ISOWeek()
	_, currentWeek := now.ISOWeek()
	if a.lastWeeklyReport.Year() == now.Year() && lastWeek == currentWeek {
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()

	// Send the weekly report
	if err := a.SendWeeklyReport(); err != nil {
		log.Printf("ERROR: failed to send weekly report: %v", err)
	}
}

// SendWeeklyReport generates and sends the weekly summary email
func (a *Alerter) SendWeeklyReport() error {
	if !a.cfg.Enabled || a.cfg.SMTPHost == "" {
		return fmt.Errorf("alerting not configured")
	}

	ctx, cancel := newTimeoutContext(30 * time.Second)
	defer cancel()

	// Get data for the past week
	reportData, err := a.fetchWeeklyReportData(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch weekly data: %w", err)
	}

	// Generate email content
	subject := fmt.Sprintf("[ProxmoxVED] Wochenbericht KW %d/%d", reportData.CalendarWeek, reportData.Year)
	body := a.generateWeeklyReportEmail(reportData)

	if err := a.sendEmail(subject, body); err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	a.mu.Lock()
	a.lastWeeklyReport = time.Now()
	a.alertHistory = append(a.alertHistory, AlertEvent{
		Timestamp: time.Now(),
		Type:      "weekly_report",
		Message:   fmt.Sprintf("Weekly report KW %d/%d sent", reportData.CalendarWeek, reportData.Year),
	})
	a.mu.Unlock()

	log.Printf("INFO: weekly report KW %d/%d sent successfully", reportData.CalendarWeek, reportData.Year)
	return nil
}

// fetchWeeklyReportData collects data for the weekly report
func (a *Alerter) fetchWeeklyReportData(ctx context.Context) (*WeeklyReportData, error) {
	// Calculate the previous week's date range (Mon-Sun)
	now := time.Now()
	
	// Find last Monday
	daysToLastMonday := int(now.Weekday() - time.Monday)
	if daysToLastMonday < 0 {
		daysToLastMonday += 7
	}
	// Go back to the Monday of LAST week
	lastMonday := now.AddDate(0, 0, -daysToLastMonday-7)
	lastMonday = time.Date(lastMonday.Year(), lastMonday.Month(), lastMonday.Day(), 0, 0, 0, 0, lastMonday.Location())
	lastSunday := lastMonday.AddDate(0, 0, 6)
	lastSunday = time.Date(lastSunday.Year(), lastSunday.Month(), lastSunday.Day(), 23, 59, 59, 0, lastSunday.Location())

	// Get calendar week
	year, week := lastMonday.ISOWeek()

	// Fetch current week's data (7 days)
	currentData, err := a.pb.FetchDashboardData(ctx, 7)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current week data: %w", err)
	}

	// Fetch previous week's data for comparison (14 days, we'll compare)
	prevData, err := a.pb.FetchDashboardData(ctx, 14)
	if err != nil {
		// Non-fatal, just log
		log.Printf("WARN: could not fetch previous week data: %v", err)
		prevData = nil
	}

	// Build report data
	report := &WeeklyReportData{
		CalendarWeek:     week,
		Year:             year,
		StartDate:        lastMonday,
		EndDate:          lastSunday,
		TotalInstalls:    currentData.TotalInstalls,
		SuccessCount:     currentData.SuccessCount,
		FailedCount:      currentData.FailedCount,
		OsDistribution:   make(map[string]int),
		TypeDistribution: make(map[string]int),
	}

	// Calculate success rate
	if report.TotalInstalls > 0 {
		report.SuccessRate = float64(report.SuccessCount) / float64(report.TotalInstalls) * 100
	}

	// Top 5 installed apps
	for i, app := range currentData.TopApps {
		if i >= 5 {
			break
		}
		report.TopApps = append(report.TopApps, AppStat{
			Name:  app.App,
			Total: app.Count,
		})
	}

	// Top 5 failed apps
	for i, app := range currentData.FailedApps {
		if i >= 5 {
			break
		}
		report.TopFailedApps = append(report.TopFailedApps, AppStat{
			Name:        app.App,
			Total:       app.TotalCount,
			Failed:      app.FailedCount,
			FailureRate: app.FailureRate,
		})
	}

	// OS distribution
	for _, os := range currentData.OsDistribution {
		report.OsDistribution[os.Os] = os.Count
	}

	// Type distribution (LXC vs VM)
	for _, t := range currentData.TypeStats {
		report.TypeDistribution[t.Type] = t.Count
	}

	// Calculate comparison to previous week
	if prevData != nil {
		// Previous week stats (subtract current from 14-day total)
		prevInstalls := prevData.TotalInstalls - currentData.TotalInstalls
		prevFailed := prevData.FailedCount - currentData.FailedCount
		prevSuccess := prevData.SuccessCount - currentData.SuccessCount

		if prevInstalls > 0 {
			prevFailRate := float64(prevFailed) / float64(prevInstalls) * 100
			currentFailRate := 100 - report.SuccessRate

			report.ComparedToPrev.InstallsChange = report.TotalInstalls - prevInstalls
			if prevInstalls > 0 {
				report.ComparedToPrev.InstallsPercent = float64(report.TotalInstalls-prevInstalls) / float64(prevInstalls) * 100
			}
			report.ComparedToPrev.FailRateChange = currentFailRate - prevFailRate
			_ = prevSuccess // suppress unused warning
		}
	}

	return report, nil
}

// generateWeeklyReportEmail creates the email body for the weekly report
func (a *Alerter) generateWeeklyReportEmail(data *WeeklyReportData) string {
	var b strings.Builder

	b.WriteString("ProxmoxVE Helper Scripts - Wöchentlicher Telemetrie-Bericht\n")
	b.WriteString("═══════════════════════════════════════════════════════════\n\n")

	b.WriteString(fmt.Sprintf("📅 Kalenderwoche: KW %d/%d\n", data.CalendarWeek, data.Year))
	b.WriteString(fmt.Sprintf("   Zeitraum: %s - %s\n\n",
		data.StartDate.Format("02.01.2006"),
		data.EndDate.Format("02.01.2006")))

	b.WriteString("────────────────────────────────────────────────────────────\n")
	b.WriteString("📊 ÜBERSICHT\n")
	b.WriteString("────────────────────────────────────────────────────────────\n\n")

	b.WriteString(fmt.Sprintf("  Installationen gesamt:  %d\n", data.TotalInstalls))
	b.WriteString(fmt.Sprintf("  ✅ Erfolgreich:         %d\n", data.SuccessCount))
	b.WriteString(fmt.Sprintf("  ❌ Fehlgeschlagen:      %d\n", data.FailedCount))
	b.WriteString(fmt.Sprintf("  📈 Erfolgsrate:         %.1f%%\n\n", data.SuccessRate))

	// Comparison to previous week
	if data.ComparedToPrev.InstallsChange != 0 || data.ComparedToPrev.FailRateChange != 0 {
		b.WriteString("  Vergleich zur Vorwoche:\n")
		changeSymbol := "📈"
		if data.ComparedToPrev.InstallsChange < 0 {
			changeSymbol = "📉"
		}
		b.WriteString(fmt.Sprintf("    %s Installationen: %+d (%.1f%%)\n",
			changeSymbol, data.ComparedToPrev.InstallsChange, data.ComparedToPrev.InstallsPercent))
		
		failChangeSymbol := "✅"
		if data.ComparedToPrev.FailRateChange > 0 {
			failChangeSymbol = "⚠️"
		}
		b.WriteString(fmt.Sprintf("    %s Fehlerrate:     %+.1f Prozentpunkte\n\n",
			failChangeSymbol, data.ComparedToPrev.FailRateChange))
	}

	b.WriteString("────────────────────────────────────────────────────────────\n")
	b.WriteString("🏆 TOP 5 INSTALLIERTE SCRIPTS\n")
	b.WriteString("────────────────────────────────────────────────────────────\n\n")

	if len(data.TopApps) > 0 {
		for i, app := range data.TopApps {
			b.WriteString(fmt.Sprintf("  %d. %-25s %5d Installationen\n", i+1, app.Name, app.Total))
		}
	} else {
		b.WriteString("  Keine Daten verfügbar\n")
	}
	b.WriteString("\n")

	b.WriteString("────────────────────────────────────────────────────────────\n")
	b.WriteString("⚠️  TOP 5 FEHLERHAFTE SCRIPTS\n")
	b.WriteString("────────────────────────────────────────────────────────────\n\n")

	if len(data.TopFailedApps) > 0 {
		for i, app := range data.TopFailedApps {
			b.WriteString(fmt.Sprintf("  %d. %-20s %3d/%3d fehlgeschlagen (%.1f%%)\n",
				i+1, app.Name, app.Failed, app.Total, app.FailureRate))
		}
	} else {
		b.WriteString("  Keine Fehler in dieser Woche! 🎉\n")
	}
	b.WriteString("\n")

	// Type distribution
	if len(data.TypeDistribution) > 0 {
		b.WriteString("────────────────────────────────────────────────────────────\n")
		b.WriteString("📦 VERTEILUNG NACH TYP\n")
		b.WriteString("────────────────────────────────────────────────────────────\n\n")
		for t, count := range data.TypeDistribution {
			percent := float64(count) / float64(data.TotalInstalls) * 100
			b.WriteString(fmt.Sprintf("  %-10s %5d  (%.1f%%)\n", strings.ToUpper(t), count, percent))
		}
		b.WriteString("\n")
	}

	// OS distribution (top 5)
	if len(data.OsDistribution) > 0 {
		b.WriteString("────────────────────────────────────────────────────────────\n")
		b.WriteString("🐧 TOP BETRIEBSSYSTEME\n")
		b.WriteString("────────────────────────────────────────────────────────────\n\n")
		
		// Sort and get top 5 OS
		type osEntry struct {
			name  string
			count int
		}
		var osList []osEntry
		for name, count := range data.OsDistribution {
			osList = append(osList, osEntry{name, count})
		}
		// Simple bubble sort for top 5
		for i := 0; i < len(osList); i++ {
			for j := i + 1; j < len(osList); j++ {
				if osList[j].count > osList[i].count {
					osList[i], osList[j] = osList[j], osList[i]
				}
			}
		}
		for i, os := range osList {
			if i >= 5 {
				break
			}
			percent := float64(os.count) / float64(data.TotalInstalls) * 100
			b.WriteString(fmt.Sprintf("  %-15s %5d  (%.1f%%)\n", os.name, os.count, percent))
		}
		b.WriteString("\n")
	}

	b.WriteString("═══════════════════════════════════════════════════════════\n")
	b.WriteString(fmt.Sprintf("Erstellt: %s\n", time.Now().Format("02.01.2006 15:04:05")))
	b.WriteString("\n---\n")
	b.WriteString("Dies ist ein automatischer Bericht des Telemetrie-Service.\n")

	return b.String()
}

// TestWeeklyReport sends a test weekly report email
func (a *Alerter) TestWeeklyReport() error {
	return a.SendWeeklyReport()
}