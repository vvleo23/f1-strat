# Analysepotenzial und Grenzen der Datenquellen

## FastF1 als Analysequelle

FastF1 eignet sich besonders für explorative und statistische Auswertungen, weil Session-, Runden-, Reifen- und Telemetriedaten direkt als Pandas-ähnliche Tabellen verfügbar sind.

### Topspeed vs. Rundenzeit

Die maximale Geschwindigkeit eines Fahrers kann mit seiner Runden- oder Sektorzeit verglichen werden. So lässt sich untersuchen, ob ein Auto zwar auf Geraden schnell ist, aber in Kurven, beim Bremsen oder bei der Traktion Zeit verliert. Dabei müssen Windschatten, DRS, Reifen, Benzinlast und Streckenbedingungen als mögliche Einflussfaktoren berücksichtigt werden.

### Fahrerduelle

Teamkollegen lassen sich innerhalb derselben Session über Rundenzeiten, Mini-Sektoren, Brems- und Beschleunigungspunkte sowie Geschwindigkeitsverläufe vergleichen. Fahrerduelle reduzieren einen Teil der Unterschiede zwischen Fahrzeugen, ersetzen aber keine Kontrolle von Reifen- und Sessionbedingungen.

### Sektorzeiten

Sektorzeiten ermöglichen eine grobe Trennung verschiedener Streckenabschnitte. In Kombination mit Telemetrie können Stärken auf Geraden, in langsamen Kurven und in schnellen Kurven genauer eingeordnet werden.

### Reifenabbau

Rundenzeiten können über einen Stint hinweg betrachtet und mit Reifenmischung, Reifenalter, Verkehr und Wetter verbunden werden. Damit sind erste Degressionsmodelle möglich. Benzinabbau und Rennsituationen müssen dabei näherungsweise kontrolliert werden.

### Wettereffekte

Strecken- und Lufttemperatur, Niederschlag, Luftfeuchtigkeit sowie Wind können mit Rundenzeiten und Reifenverhalten verknüpft werden. Für robuste Aussagen sind vergleichbare Streckenbedingungen und ausreichend viele Sessions erforderlich.

### Fahrzeugprofile über Streckentypen

Ergebnisse und Telemetriemerkmale mehrerer Strecken können zu Fahrzeugprofilen zusammengeführt werden. Denkbar ist eine Einteilung nach Low-, Medium- und High-Downforce-Strecken oder nach Kurven- und Geradenanteil. So lässt sich prüfen, ob ein Fahrzeug auf bestimmten Streckentypen systematisch stärker ist.

## Bewertete Anwendungsfälle

### Dynamische Rennstrategie

Rundenzeiten, Reifenstints, Wetter, Positionen, Boxenstopps und Race-Control-Meldungen reichen für ein historisches Point-in-Time-Replay aus. Dabei wird der Informationsstand am Ende jeder Runde rekonstruiert und eine feste Vor-Renn-Strategie mit laufend aktualisierten Stoppszenarien verglichen. Pace, Reifendegradation, Pit Loss und Verkehr müssen als unsichere Größen behandelt werden.

Diese Fragestellung nutzt die größte Schnittmenge der bereits geprüften Daten und besitzt eine klare offene Lücke: Öffentliche Projekte lösen einzelne Simulations-, Reifen- oder Optimierungsprobleme, aber nicht nachgewiesen die Kombination aus öffentlichen Daten, rundenweiser Neuplanung und leakage-freiem Backtesting.

### Qualifying Predictor

Trainingspace, Wetter, bisherige Saisonleistung und repräsentative Soft-Reifen-Runs können für Pole-Zeit, Startpositionen oder Q3-Wahrscheinlichkeiten verwendet werden. Der Ansatz ist gut evaluierbar, besitzt aber bereits mehrere öffentliche klassische und bayesianische Implementierungen. Ein eigener Beitrag sollte deshalb probabilistische, kalibrierte Vorhersagen und saisonweise Walk-forward-Tests statt nur eines einzelnen Regressionsmodells liefern.

### Bewertung der Fahrzeuggeneration 2026

OpenF1-, FastF1-, Jolpica- und F1DB-Daten ermöglichen eine Zwischenbilanz zu Überholmanövern, Führungswechseln, engen Abständen, Feldspreizung und Wettbewerbsverteilung. Eine belastbare Aussage benötigt mindestens eine vollständige Saison und Kontrollen für Strecke, Wetter, Safety Car, Ausfälle und Kalender. Die Wirkung des Reglements kann mit Beobachtungsdaten beschrieben, aber nur eingeschränkt kausal isoliert werden.

## OpenF1 als Analyse- und Integrationsquelle

OpenF1 eignet sich vor allem dort, wo Daten über eine einfache HTTP-Schnittstelle abgefragt und an andere Systeme weitergegeben werden sollen.

### API-basierte Datenabfragen

Sessions und einzelne Datendomänen lassen sich per URL und Query-Parameter abrufen. Dadurch können Datenabrufe unabhängig von einer speziellen Python-Bibliothek automatisiert werden.

### Race-Replay-Dashboard

Zeitlich aufgelöste Fahrzeug- und Positionsdaten bilden eine Grundlage für ein späteres Race-Replay-Dashboard. Datenmenge, zeitliche Synchronisierung und API-Verfügbarkeit müssen dafür gesondert geprüft werden.

### Positionen, Pitstops, Race Control und Wetter

OpenF1 stellt eigenständige Endpunkte für Rennpositionen, Boxenstopps, Race-Control-Meldungen und Wetter bereit. Diese Daten sind hilfreich, um Rennverläufe, Neutralisierungen und strategische Ereignisse zu erklären.

### JSON/CSV-Pipeline für eine spätere Web-App

Die JSON-Antworten lassen sich direkt normalisieren und als CSV oder Parquet persistieren. Damit ist OpenF1 gut als Eingangsquelle für eine ETL-Pipeline oder eine spätere Web-App geeignet.

## Datenlücken und methodische Grenzen

Beide Quellen liefern beobachtete Session- und Fahrzeugdaten, aber keine vollständigen technischen Fahrzeugmodelle. Insbesondere fehlen:

- ein echter, direkt gemessener Downforce-Wert
- ein echter Luftwiderstands- beziehungsweise Drag-Wert
- die tatsächliche Motorleistung
- exakte Fahrzeug-Setups wie Flügelwinkel, Bodenhöhe oder Fahrwerksabstimmung
- die genaue Benzinmenge zu jedem Zeitpunkt
- frei verfügbare echte Realtime-Daten bei OpenF1

Deshalb kann die Frage, was ein F1-Auto schnell macht, nur über Indikatoren und kontrollierte Vergleiche untersucht werden. Topspeed allein ist kein Beweis für geringen Luftwiderstand, und Kurvengeschwindigkeit allein ist kein direkter Downforce-Messwert. Reifen, Fahrer, Verkehr, DRS, Wind, Benzinlast und Streckenentwicklung müssen in der Interpretation berücksichtigt werden.

## Erste Empfehlung

**Der dynamische Rennstrategie-Simulator sollte als Hauptfragestellung verfolgt werden.** Der erste belastbare Schritt ist ein historisches Point-in-Time-Replay; ein echtes Live-System ist erst nach erfolgreichen Backtests sinnvoll.

**FastF1 sollte den Analyse-Kern bilden.** Die Bibliothek bietet den bequemeren Zugang zu Runden, Sektoren, Reifen und detaillierter Telemetrie. OpenF1 ergänzt zeitnahe Sessions, eigenständige Ereignisendpunkte und später optional eine Live-Anbindung.

Die vollständige Bewertung öffentlicher Repositories, kommerzieller Produkte und verbleibender Lücken steht in [`KONKURRENZANALYSE.md`](KONKURRENZANALYSE.md). Ein vollständiger Quellenvergleich ist weiterhin erforderlich, bevor Simulationsgüte oder Live-Fähigkeit bewertet werden können.

