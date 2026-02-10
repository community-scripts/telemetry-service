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
}

// Alerter handles alerting functionality
type Alerter struct {
	cfg          AlertConfig
	lastAlertAt  time.Time
	mu           sync.Mutex
	pb           *PBClient
	lastStats    alertStats
	alertHistory []AlertEvent
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