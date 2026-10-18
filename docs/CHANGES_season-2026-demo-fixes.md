# Branch `feature/season-2026-demo-fixes`: Änderungen gegenüber `main`

Basis `main` 41c9f5b. 17 Dateien, 146 Unit-Tests grün. Details und Belege stehen in `docs/projektdokumentation.md`, Abschnitt 4 „Änderungen und Befunde vom 15. bis 18. Oktober 2026“.

## Eingriffe in bestehende Logik

Kein Umbau der Pipeline. Quellenanbindung, Snapshots, Manifeste, Parquet-Schemas, Zeitlogik (`decision_time`/`available_at`), Job-Service mit Sperre und Speicherung sind unverändert. In `src/` kommen rund 1.360 Zeilen hinzu (davon 889 Admin-Panel, ca. 300 Ausfallregel); nur 18 bestehende Zeilen werden ersetzt oder entfernt.

| Stelle | Eingriff | Ausmaß |
|---|---|---|
| Strategie `build_pace_profile` | Mischungsabstand und Abbau aus Longruns statt aus allen Runden | einzige echte Modelländerung; ändert alle Strategiewerte (v6) |
| Rennstatus (Strategie + Re-live) | Bedingungen für VSC, rote Flagge, Phasenende korrigiert | 6 Bedingungen, Ablauf gleich |
| Re-live Boxenprojektion | Abstände nur noch für aktive Autos verlangt | 1 Bedingung |
| Re-live Zeitabgleich | Runden ohne Startzeit vorher aussortiert | 1 Filter |
| Ingestion OpenF1 | Stints ohne Start- und Endrunde verworfen, im Manifest vermerkt | 1 Filter vor der unveränderten Validierung |
| Re-live-Anzeige (JS) | ausgefallene Autos grau eingefroren statt ausgeblendet | 1 Zeichenregel |

Alles Weitere ist Ergänzung ohne Änderung bestehenden Verhaltens: Admin-Panel, Ausfallregel (alte Bewegungsregel bleibt, neue kommt per „oder“ dazu), optionaler Fortschritts-Callback im Job-Runner, Streckenzuordnungen (nur Konfiguration).

## Alle Änderungen

**Schwere:** gering = lokal, keine Auswirkung auf gespeicherte Ergebnisse · mittel = sichtbares Verhalten ändert sich · hoch = Rechenergebnisse ändern sich, neue Version.

| Bereich | Was | Warum | Schwere |
|---|---|---|---|
| Rennstatus (Re-live + Strategie) | „VSC DEPLOYED/ENDING“ erkannt; zweite VSC/SC-Phase erscheint; „CHEQUERED FLAG“ und Stewards-Text („… RED FLAG INFRINGEMENT“) gelten nicht mehr als rote Flagge | OpenF1 kürzt VSC seit 2026 ab; vorher fehlten in 2026 alle VSC-Phasen, jede zweite Neutralisierung, und jedes Rennen endete auf RED. Danach stimmen alle 23 Rennen/Sprints 2026 mit Race Control überein | mittel |
| Strategie: Reifenprofil | Mischungsabstand und Abbau nur aus Longruns (≤ 107 % Bestzeit, ≥ 5 Runden je Stint) | Abkühl- und Inlaps aus Quali-Simulationen machten Soft 1,4 s langsamer als Medium; Stopps landeten immer an der Fenstergrenze. Belgien danach 0 bis −4 Runden zum echten Stopp | hoch: `race_strategy_rules_v6`, `features_v2`, alle Strategien neu |
| Re-live: Ausfallregel | Zusatzregel markiert ein Auto als ausgefallen, wenn Zeitnahme, Positionen und Race Control das belegen (mind. zwei Signale); Auto bleibt grau am Unfallort stehen | OpenF1-Ortung lässt verunfallte Autos weitergleiten (Monza 2026, Leclerc). Bewegungsregel bleibt unverändert. Nur zum Zeitpunkt verfügbare Daten; Saisonprüfung 23 Sessions: 1 richtiger Treffer, 1 Fehlalarm (Quelldatenfehler, nach 56 s zurückgenommen) | mittel |
| Admin-Panel „Pipeline activity“ (neu) | Read-only-Ansicht: Kennzahlen, laufende Jobs mit Endpoint-Fortschritt, Live-Feed, Saisonabdeckung je Quelle, Zeitachse, filterbare Tabelle; erkennt hängende Jobs | Laden ganzer Saisons war nicht nachvollziehbar; Job-Runner schreibt dafür `job_<id>.progress.json` (optionaler Callback `on_progress`) | gering |
| Streckenzuordnungen | 11 geprüfte Wikidata-Zuordnungen in `config/reviewed_circuit_mappings.json` (u. a. Spielberg, Monaco, Barcelona, Interlagos) | Autosuche über den OpenF1-Kurznamen findet Orte statt Strecken oder mehrere Strecken; ohne Koordinate kein Wetter. Danach alle 17 Wochenenden 2026 mit Forecast. Code unverändert | gering (nur Konfiguration) |
| Robustheit Ingestion/Re-live | Stints von Nichtstartern verworfen; Runden ohne Startzeit übersprungen; Boxenprojektion ignoriert inaktive Autos; Schreibfehler der Fortschrittsdatei brechen keinen Job mehr ab | Einzelne Quelldatenfälle blockierten ganze Rennen (u. a. São Paulo 2024, Australien/China 2026) | gering |
| Doku | Projektdoku §4–6 und README nachgezogen | Nachvollziehbarkeit | gering |

**Für das Team wichtig**

- Strategie-Ergebnisse unter v4/v5 sind mit diesem Stand nicht reproduzierbar; Zahlen in Arbeit und Folien (u. a. Ungarn §6.5) unter v6 neu ziehen.
- Keine Schema- oder Datenmigration, keine neuen Abhängigkeiten. Lokale Daten bleiben gültig; Wetter für neue Zuordnungen per Wochenend-Neuladen.
- Bekannte Grenzen (nicht geändert, dokumentiert): Reifenwerte aus Trainings zu hoch (unnötiger zweiter Stopp), Boxenverlust weicht teils von MultiViewer ab, offene Runden im Re-live mit kürzerer Startrunde normiert.
