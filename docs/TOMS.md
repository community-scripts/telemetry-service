# Technische und Organisatorische Maßnahmen (TOM)

**gemäß Art. 32 DSGVO**

---

## 1. Vertraulichkeit (Art. 32 Abs. 1 lit. b DSGVO)

### 1.1 Zutrittskontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Rechenzentrum | Hetzner Cloud (ISO 27001 zertifiziert) | ✅ |
| Physischer Zugriff | Durch Hetzner gesichert (Biometrie, 24/7 Überwachung) | ✅ |

### 1.2 Zugangskontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| SSH-Zugang | Nur mit SSH-Keys, kein Passwort-Login | ✅ |
| API-Authentifizierung | PocketBase Admin-Token erforderlich | ✅ |
| Dashboard-Zugriff | Lesezugriff ohne Authentifizierung (nur aggregierte Daten) | ✅ |
| Admin-Zugriff | Über Coolify mit 2FA | ✅ |

### 1.3 Zugriffskontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Berechtigungskonzept | Minimalprinzip: Service hat nur Schreibrechte auf telemetry-Collection | ✅ |
| API-Endpunkte | Telemetrie-Endpoint: Nur POST, Dashboard-API: Nur GET | ✅ |
| Keine Root-Prozesse | Container läuft mit non-root User | ✅ |

### 1.4 Trennungskontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Datentrennung | Separate Collections für ProxmoxVE/ProxmoxVED | ✅ |
| Netzwerktrennung | Docker-Network-Isolation | ✅ |
| Umgebungstrennung | Produktion getrennt von Entwicklung | ✅ |

---

## 2. Integrität (Art. 32 Abs. 1 lit. b DSGVO)

### 2.1 Weitergabekontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Transportverschlüsselung | TLS 1.3 (HTTPS) | ✅ |
| Interne Kommunikation | Docker-internes Netzwerk | ✅ |
| Keine Drittland-Übermittlung | Server ausschließlich in Deutschland | ✅ |

### 2.2 Eingabekontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Request-Validierung | Strikte JSON-Schema-Validierung | ✅ |
| Max Body Size | 1024 Bytes (verhindert Oversized Payloads) | ✅ |
| Fehlermeldungen | Max. 120 Zeichen (verhindert Log-Injection) | ✅ |
| Audit-Logging | Fehlerhafte Anfragen werden geloggt (ohne IP) | ✅ |

---

## 3. Verfügbarkeit und Belastbarkeit (Art. 32 Abs. 1 lit. b/c DSGVO)

### 3.1 Verfügbarkeitskontrolle
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Health-Checks | `/health`-Endpoint mit Docker HEALTHCHECK | ✅ |
| Auto-Restart | Coolify startet Container bei Absturz neu | ✅ |
| Rate Limiting | 60 Requests/Minute pro IP (DDoS-Schutz) | ✅ |
| Timeout-Handling | 120s Timeout für Dashboard-Queries | ✅ |

### 3.2 Wiederherstellbarkeit
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| Datensicherung | PocketBase SQLite-Backups durch Coolify | ✅ |
| Backup-Intervall | Täglich | ✅ |
| Disaster Recovery | Daten können aus Backup wiederhergestellt werden | ✅ |

---

## 4. Verfahren zur regelmäßigen Überprüfung (Art. 32 Abs. 1 lit. d DSGVO)

### 4.1 Datenschutz-Management
| Maßnahme | Umsetzung | Status |
|----------|-----------|--------|
| VVT vorhanden | [docs/VVT.md](VVT.md) | ✅ |
| Security Policy | [SECURITY.md](../SECURITY.md) | ✅ |
| Löschkonzept | Automatische Löschung nach 365 Tagen | ✅ |

### 4.2 Technische Prüfungen
| Maßnahme | Intervall | Status |
|----------|-----------|--------|
| Dependency-Updates | Bei jedem Build (Go Modules) | ✅ |
| Container-Updates | Alpine-Base regelmäßig aktualisiert | ✅ |
| Code-Review | Alle Änderungen via Pull Request | ✅ |

---

## 5. Privacy by Design / Privacy by Default (Art. 25 DSGVO)

### 5.1 Privacy by Design
| Prinzip | Umsetzung | Status |
|---------|-----------|--------|
| Datenminimierung | Nur technisch notwendige Daten werden erhoben | ✅ |
| Anonymität | Keine personenbezogenen Daten, anonyme Session-IDs | ✅ |
| Keine IP-Speicherung | `ENABLE_REQUEST_LOGGING=false` | ✅ |

### 5.2 Privacy by Default
| Einstellung | Standard | Status |
|-------------|----------|--------|
| Telemetrie | Opt-In (Nutzer muss aktiv zustimmen) | ✅ |
| Request-Logging | Deaktiviert | ✅ |
| Datenweitergabe | Keine | ✅ |

---

## 6. Auftragsverarbeitung

### 6.1 Dienstleister
| Dienstleister | Funktion | Standort | Vertrag |
|---------------|----------|----------|---------|
| Hetzner Cloud | Infrastructure | Deutschland | AV-Vertrag vorhanden |
| Coolify | Container-Orchestrierung | Self-Hosted | - |
| GitHub | Source Code Hosting | USA | DPF-zertifiziert |

### 6.2 Keine Weitergabe an Dritte
Die Telemetriedaten werden **nicht** an externe Analysedienste, Werbepartner oder sonstige Dritte weitergegeben.

---

## 7. Technische Schutzmaßnahmen im Code

```go
// service.go - Security Headers
w.Header().Set("X-Content-Type-Options", "nosniff")
w.Header().Set("X-Frame-Options", "DENY")
w.Header().Set("Referrer-Policy", "no-referrer")

// Rate Limiting
RateLimitRPM: 60       // Max 60 Requests pro Minute
RateBurst:    20       // Burst-Limit
MaxBodyBytes: 1024     // Max 1KB Request-Body

// Keine IP-Speicherung
EnableReqLogging: false
```

---

## 8. Maßnahmen bei Datenschutzverletzungen

| Schritt | Verantwortlich | Frist |
|---------|----------------|-------|
| Erkennung | Automatisch (Monitoring) oder via GitHub Issue | - |
| Ersteinschätzung | Maintainer | 24 Stunden |
| Meldung an Aufsichtsbehörde | N/A (keine personenbezogenen Daten) | - |
| Benachrichtigung Betroffener | N/A (keine personenbezogenen Daten) | - |
| Dokumentation | GitHub Security Advisory | 7 Tage |

---

## 9. Änderungshistorie

| Datum | Version | Änderung | Autor |
|-------|---------|----------|-------|
| 2026-02-12 | 1.0 | Initiale Erstellung | Community Scripts Team |

---

*Diese Dokumentation wird bei wesentlichen Änderungen am Service aktualisiert.*
