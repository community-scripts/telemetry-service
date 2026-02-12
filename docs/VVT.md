# Verzeichnis von Verarbeitungstätigkeiten (VVT)

**gemäß Art. 30 DSGVO**

---

## 1. Angaben zum Verantwortlichen

| Feld | Wert |
|------|------|
| **Organisation** | Community Scripts (Open Source Projekt) |
| **Projektname** | Telemetry Service |
| **Repository** | https://github.com/community-scripts/telemetry-service |
| **Kontakt** | Über GitHub Issues |

---

## 2. Zweck der Verarbeitung

### 2.1 Beschreibung
Der Telemetry Service sammelt **anonyme technische Nutzungsstatistiken** von den Proxmox VE Helper-Scripts. Diese Daten dienen ausschließlich der:

- **Qualitätsverbesserung**: Identifikation von Scripts mit hohen Fehlerraten
- **Priorisierung**: Erkennung der meistgenutzten Anwendungen
- **Trendanalyse**: Verständnis der verwendeten Betriebssysteme und Ressourcenkonfigurationen

### 2.2 Rechtsgrundlage
**Art. 6 Abs. 1 lit. f DSGVO** (berechtigtes Interesse)

Das berechtigte Interesse liegt in der Verbesserung der Open-Source-Software für die Community. Die Verarbeitung ist minimal-invasiv, da:
- Keine personenbezogenen Daten erhoben werden
- Keine IP-Adressen gespeichert werden
- Die Datenübermittlung opt-in ist (Nutzer müssen aktiv zustimmen)

---

## 3. Kategorien betroffener Personen

| Kategorie | Beschreibung |
|-----------|--------------|
| Nutzer der Helper-Scripts | Administratoren, die Proxmox VE Helper-Scripts ausführen und der Telemetrie zugestimmt haben |

---

## 4. Kategorien personenbezogener Daten

### ⚠️ KEINE personenbezogenen Daten werden erhoben

Die erhobenen Daten sind **ausschließlich technischer Natur** und lassen **keinen Rückschluss auf natürliche Personen** zu:

| Datenfeld | Typ | Beschreibung | Personenbezug |
|-----------|-----|--------------|---------------|
| `random_id` | UUID | Zufällig generierte Session-ID (pro Installation neu) | ❌ Nein |
| `type` | String | LXC, VM, Tool, Addon | ❌ Nein |
| `nsapp` | String | Name der installierten Anwendung (z.B. "jellyfin") | ❌ Nein |
| `status` | String | Erfolgreich / Fehlgeschlagen / Installierend | ❌ Nein |
| `disk_size` | Integer | Festplattengröße in GB | ❌ Nein |
| `core_count` | Integer | CPU-Kerne | ❌ Nein |
| `ram_size` | Integer | RAM in MB | ❌ Nein |
| `os_type` | String | Betriebssystem (debian, ubuntu, alpine) | ❌ Nein |
| `os_version` | String | OS-Version (12, 24.04) | ❌ Nein |
| `pve_version` | String | Proxmox VE Version | ❌ Nein |
| `method` | String | Installationsmethode | ❌ Nein |
| `error` | String | Fehlerbeschreibung (max. 120 Zeichen) | ❌ Nein |
| `exit_code` | Integer | Exit-Code (0-255) | ❌ Nein |
| `gpu_vendor` | String | GPU-Hersteller | ❌ Nein |
| `cpu_vendor` | String | CPU-Hersteller | ❌ Nein |
| `install_duration` | Integer | Installationsdauer in Sekunden | ❌ Nein |

### Was wird NICHT erhoben:
- ❌ IP-Adressen (Request-Logging deaktiviert)
- ❌ Hostnamen oder Domainnamen
- ❌ MAC-Adressen oder Seriennummern
- ❌ Benutzernamen oder E-Mail-Adressen
- ❌ Netzwerkkonfiguration
- ❌ Standortdaten

---

## 5. Empfänger der Daten

| Empfänger | Zweck | Rechtsgrundlage |
|-----------|-------|-----------------|
| PocketBase (selbst gehostet) | Speicherung der Telemetriedaten | Auftragsverarbeitung (gleicher Server) |
| GitHub (für öffentliches Dashboard) | Aggregierte Statistiken | Art. 6 Abs. 1 lit. f (berechtigtes Interesse) |

**Keine Weitergabe an Dritte.** Die Daten werden ausschließlich für die Verbesserung der Helper-Scripts verwendet.

---

## 6. Übermittlung in Drittländer

| Drittland | Übermittlung | Garantien |
|-----------|--------------|-----------|
| USA | ❌ Nein | - |
| Andere | ❌ Nein | - |

Die Datenverarbeitung erfolgt **ausschließlich auf EU-Servern** (Hetzner Cloud, Deutschland).

---

## 7. Löschfristen

| Datenkategorie | Löschfrist | Begründung |
|----------------|------------|------------|
| Telemetriedaten | **365 Tage** | Ausreichend für jährliche Trendanalysen |
| Aggregierte Statistiken | Unbegrenzt | Keine personenbezogenen Daten |
| Logs (falls aktiviert) | 7 Tage | Technische Fehlerbehebung |

Die automatische Löschung wird durch den `cleanup`-Job im Service umgesetzt.

---

## 8. Technische und organisatorische Maßnahmen (TOM)

Siehe separate Dokumentation: [TOMS.md](TOMS.md)

**Zusammenfassung:**
- ✅ Verschlüsselung in Transit (TLS 1.3)
- ✅ Zugriffskontrolle (API-Token-basiert)
- ✅ Rate Limiting (DDoS-Schutz)
- ✅ Keine IP-Speicherung
- ✅ Privacy by Design (anonyme Session-IDs)

---

## 9. Datenschutz-Folgenabschätzung (DSFA)

Eine DSFA nach Art. 35 DSGVO ist **nicht erforderlich**, da:

1. Keine personenbezogenen Daten verarbeitet werden
2. Kein Profiling oder automatisierte Entscheidungsfindung stattfindet
3. Keine besonderen Kategorien personenbezogener Daten (Art. 9 DSGVO) betroffen sind
4. Die Verarbeitung kein hohes Risiko für die Rechte und Freiheiten natürlicher Personen darstellt

---

## 10. Änderungshistorie

| Datum | Version | Änderung | Autor |
|-------|---------|----------|-------|
| 2026-02-12 | 1.0 | Initiale Erstellung | Community Scripts Team |

---

*Dieses Dokument wurde nach bestem Wissen und Gewissen erstellt. Bei Fragen oder Änderungswünschen kontaktieren Sie uns über GitHub Issues.*
