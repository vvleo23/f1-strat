# F1 API Validation

Dieses Python-3.14-Projekt prüft, welche Daten **FastF1** und **OpenF1** für ein späteres Formel-1-Data-Analytics-Projekt bereitstellen. Nach einer Bewertung mehrerer Fragestellungen liegt der aktuelle Schwerpunkt auf einem **dynamischen F1-Rennstrategie-Simulator**, der Reifen- und Boxenstoppentscheidungen in historischen Point-in-Time-Replays laufend aktualisiert.

Das Repository dient zunächst der technischen und inhaltlichen Machbarkeitsprüfung. Es baut noch keine finale Simulation, Live-Anwendung oder Weboberfläche. Die ursprüngliche Fahrzeugperformance-Frage bleibt als möglicher Analysebaustein erhalten.

## Datenquellen

### FastF1

[FastF1](https://docs.fastf1.dev/) ist eine Python-Bibliothek für historische Formel-1-Daten. Sie stellt unter anderem Rundenzeiten, Sektoren, Reifeninformationen, Session-Ergebnisse, Wetter und hochfrequente Fahrzeugtelemetrie bereit. FastF1 bildet deshalb den Schwerpunkt für spätere analytische Auswertungen.

### OpenF1

[OpenF1](https://openf1.org/) bietet Formel-1-Daten über eine HTTP-API an. Die JSON-Antworten eignen sich besonders für reproduzierbare Datenpipelines, Dashboards und spätere Webanwendungen. Historische Daten sind frei nutzbar; echte Realtime-Daten gehören nicht zum frei verfügbaren Umfang.

## Projektstruktur

```text
f1-api-validation/
├── cache/
├── data/
│   ├── raw/
│   └── processed/
├── reports/
│   ├── KONKURRENZANALYSE.md
│   ├── README_ANALYSIS.md
│   └── UMSETZBARKEIT.md
├── src/
│   ├── __init__.py
│   ├── circle_of_doom.py
│   ├── config.py
│   ├── fastf1_probe.py
│   ├── openf1_probe.py
│   └── compare_sources.py
├── tests/
│   └── test_circle_of_doom.py
├── requirements.txt
├── README.md
└── .gitignore
```

Die Skripte erzeugen die Arbeitsverzeichnisse bei Bedarf automatisch. Cache- und Datendateien werden nicht in Git aufgenommen.

## Installation

### 1. Python 3.14 verwenden

```bash
python3.14 --version
```

### 2. Virtuelle Umgebung erstellen und aktivieren

```bash
python3.14 -m venv .venv
source .venv/bin/activate
```

### 3. Abhängigkeiten installieren

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Skripte starten

```bash
python src/fastf1_probe.py
python src/openf1_probe.py
python src/compare_sources.py
```

Die ersten Abrufe können abhängig von Netzwerk und Datenmenge einige Zeit dauern. FastF1 legt heruntergeladene Inhalte im lokalen Verzeichnis `cache/` ab. Parquet-Ausgaben werden unter `data/raw/` gespeichert.

## Circle of Doom: Belgien 2026

Das interaktive Replay für das Rennen in Spa-Francorchamps am 19. Juli 2026 wird so erzeugt:

```bash
python src/circle_of_doom.py \
  --session-key 11334 \
  --driver ANT \
  --frame-seconds 4 \
  --max-staleness 8 \
  --open
```

Die erste Ausführung lädt Runden, Abstände, Positionen, Stints, Boxenstopps, Race Control und ungefähr 721.000 `location`-Messungen. Die Location-Daten werden resumierbar je Fahrer unter `data/raw/` gespeichert. Weitere Ausführungen verwenden den lokalen Parquet-Cache.

Die HTML-Datei entsteht standardmäßig unter `data/processed/circle_of_doom_session_11334_ant.html`. Mit `--self-contained` wird Plotly in die Datei eingebettet, sodass die Visualisierung anschließend keine CDN-Verbindung benötigt.

Standardmäßig werden Keyframes im Abstand von vier Sekunden rekonstruiert. Das entspricht ungefähr der Aktualisierungsfrequenz der OpenF1-Abstandsdaten. Zwischen zwei Keyframes bewegen sich alle Fahrzeuge browserseitig kontinuierlich entlang des Kreises; dadurch bleibt das Replay flüssig, ohne zusätzliche Messgenauigkeit vorzutäuschen. Die Schaltflächen **1×**, **2×**, **5×** und **10×** geben das Rennen in Echtzeit oder beschleunigt wieder. **Pause** hält auch den aktuellen Zwischenstand an. Über den Zeitregler kann jederzeit zu einem anderen Rennabschnitt gesprungen werden.

### Bedeutung des Kreises

- **oben:** Start/Ziel, also 0 beziehungsweise 100 Prozent der Runde,
- **unten:** 50 Prozent der geometrischen Rundendistanz,
- **im Uhrzeigersinn:** Fahrtrichtung durch die Runde,
- **Fahrzeugmarker:** aus der kumulierten x/y/z-Weglänge innerhalb der jeweiligen Runde,
- **Hovertext:** Position, Zeitabstand zum Führenden, Runde, Fortschritt und Reifen,
- **gelbes `PIT`-Kreuz:** prognostizierte Rundenuhrposition nach einem sofortigen Stopp,
- **gelber Bogen:** angenommener Zeitverlust des Stopps.

Der normale Pit Loss beträgt standardmäßig 20 Sekunden und kann mit `--pit-loss` geändert werden. Unter SC/VSC werden standardmäßig 12 Sekunden verwendet; dieser Wert ist über `--neutralized-pit-loss` konfigurierbar.

Die Fahrzeugposition auf dem Kreis stammt aus den ungefähren öffentlichen OpenF1-Koordinaten und ist keine hochpräzise GPS-Messung. Die Pit-Position ist eine Projektion aus Pit Loss und Referenz-Rundenzeit; sie ersetzt kein detailliertes Modell von Boxenein- und -ausfahrt, Out-Lap, Verkehr oder Reifenaufwärmung.

## Validierte Daten

Das Projekt prüft exemplarisch die Verfügbarkeit und Verarbeitbarkeit folgender Datengruppen:

- Rundenzeiten und Sektorinformationen
- Fahrzeugtelemetrie wie Geschwindigkeit, Gas, Bremse, Gang und Drehzahl
- Wetterdaten
- Reifen und Stints
- Pitstops
- Race-Control-Daten
- Positionen auf der Runde aus OpenF1-x/y/z-Koordinaten

Der erste Vergleich in `compare_sources.py` ist bewusst nur ein Plausibilitätscheck der FastF1-Rundendaten. Ein feldweiser Abgleich beider Quellen kann auf dieser Grundlage später ergänzt werden.

## Projektbewertung und Analysehinweise

- [`reports/UMSETZBARKEIT.md`](reports/UMSETZBARKEIT.md) bewertet Datenquellen, Aktualität, Kosten, Grenzen und die priorisierte Forschungsfrage.
- [`reports/KONKURRENZANALYSE.md`](reports/KONKURRENZANALYSE.md) vergleicht öffentliche Repositories, wissenschaftliche Arbeiten und geschlossene Produkte und leitet den empfohlenen MVP ab.
- [`reports/README_ANALYSIS.md`](reports/README_ANALYSIS.md) fasst Analysepotenziale und methodische Grenzen kompakt zusammen.

Die favorisierte Forschungsfrage lautet:

> Wie stark reduziert eine nach jedem Rundenende mit ausschließlich bis dahin beobachteten öffentlichen Daten aktualisierte Reifen- und Boxenstoppstrategie die erwartete verbleibende Rennzeit gegenüber einem vor dem Start festgelegten Strategieplan?

