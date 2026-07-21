# Konkurrenzanalyse und Forschungslandschaft

**Stand:** 21. Juli 2026  
**Schwerpunkt:** Dynamischer F1-Rennstrategie-Simulator  
**Weitere Kandidaten:** Qualifying Predictor und Bewertung der Fahrzeuggeneration 2026

## Kurzfazit

Es gibt mehrere wertvolle Bausteine, aber keine öffentlich nachvollziehbare Lösung, die das angestrebte Problem vollständig abdeckt:

- **TUMFTM/race-simulation** ist der wissenschaftlich am besten dokumentierte offene Rennsimulator. Er modelliert Reifenabbau, Kraftstoffeffekt, Gegner und Zufallsereignisse und enthält mehrere Varianten eines virtuellen Strategieingenieurs. Die enthaltenen F1-Parameter stammen jedoch überwiegend aus 2014 bis 2019; die RL-Variante muss je Rennen trainiert werden und unterstützt keine Nassrennen. Eine laufende öffentliche Datenquelle wird nicht als operativer Live-Eingang verwendet.
- **Formula-One-Strategy-Sim** ist konzeptionell am nächsten an der geplanten Architektur: historische Ingestion, kanonischer Race State, rundenweises Replay, Top-K-Rollouts, Gegenfaktuale und Erklärungen. Das Projekt beschränkt sich ausdrücklich auf historische Replays, ist sehr jung und weist in den GitHub-Metadaten keine Lizenz aus. Seine Konzepte sind lehrreich; eine Übernahme von Code ist ohne geklärte Lizenz nicht vorgesehen.
- **F1 Strategy Simulator 2025** zeigt, wie sich reale FastF1-Daten, bayesianische Reifendegradation und Monte-Carlo-Vergleiche verbinden lassen. Er simuliert aber freie Fahrt ohne Wetter, Neutralisierungen, Verkehr und Gegnerreaktionen und ist archiviert.
- **F1 Pit Stop Strategy Simulator** demonstriert eine Gymnasium-/RL-Struktur mit Wetter, Safety Car und Verkehr. Die Modelle werden jedoch mit synthetischen Simulationsdaten trainiert; reale F1-Datenintegration, Mehrfahrersimulation und dynamische Neuplanung waren beim geprüften Stand noch Roadmap-Punkte.
- **RaceWatch** und **F1 Insights powered by AWS** belegen, dass dynamische Strategieanalyse praktisch relevant und technisch machbar ist. Beide sind geschlossen und greifen teilweise auf wesentlich reichhaltigere beziehungsweise nicht öffentliche Daten zu.

Die verbleibende Forschungs- und Umsetzungslücke lautet daher nicht „noch ein statischer Strategie-Vergleich“, sondern:

> Ein reproduzierbarer, auf öffentlichen Daten basierender Simulator, der zu jedem Rundenende ausschließlich den damals bekannten Informationsstand rekonstruiert, Pace und Reifenzustand samt Unsicherheit aktualisiert und robuste Strategiealternativen gegen einen vor dem Rennen festgelegten Plan bewertet.

## Untersuchungsmethode und Grenzen

Die Bewertung basiert auf öffentlich sichtbaren README-Dateien, Projektdokumentationen, GitHub-Metadaten und Produktseiten. Repository-Aktivität, Sterne und Roadmaps sind nur Indikatoren; sie belegen weder Modellqualität noch produktive Einsatzfähigkeit. Aussagen zu geschlossenen Produkten beruhen auf deren Herstellerbeschreibungen, weil Implementierung, Trainingsdaten und unabhängige Benchmarks nicht öffentlich sind.

Für jedes Produkt wurden folgende Fragen betrachtet:

1. Nutzt es reale oder ausschließlich simulierte Daten?
2. Modelliert es Reifen, Pace, Wetter, Verkehr und Neutralisierungen?
3. Rechnet es Gegnerreaktionen und Unsicherheit ein?
4. Kann es während eines Replays oder Rennens neu planen?
5. Verhindert seine Evaluation Zukunftswissen und Data Leakage?
6. Sind Code, Parameter, Datenherkunft und Lizenz nachvollziehbar?

## Öffentliche Rennstrategie-Projekte

### TUMFTM/race-simulation

**Repository:** <https://github.com/TUMFTM/race-simulation>  
**Lizenz:** LGPL-3.0  
**Wissenschaftliche Basis:** Veröffentlichungen von Heilmeier et al. aus 2018 und 2020

**Was gelöst wird**

- rundenweise Simulation vollständiger Rundstreckenrennen,
- Reifenabbau und Massenreduktion durch Kraftstoffverbrauch,
- Interaktionen zwischen Rennteilnehmern,
- probabilistische Ereignisse und Monte-Carlo-Auswertung,
- Basisstrategie für minimale Rennzeit auf freier Strecke,
- virtueller Strategieingenieur mit realer Strategie, Regeln, überwachten neuronalen Netzen oder Reinforcement Learning.

**Offene Lücken für dieses Projekt**

- Die mitgelieferten F1-Parameter decken vor allem Rennen von 2014 bis 2019 ab.
- Der RL-Agent wird strecken- beziehungsweise rennspezifisch trainiert.
- Die bereitgestellten RL-Agenten behandeln nur Trockenreifen.
- Eine Simulation von Live-Verläufen ist nicht gleichbedeutend mit dem laufenden Einlesen und Kalibrieren anhand beobachteter FastF1-/OpenF1-Daten.
- Die Dokumentation weist selbst darauf hin, dass reale Rennen nicht exakt reproduziert werden können.

**Was übernommen werden kann**

- klare Trennung zwischen Basisstrategie und interaktiver Rennsimulation,
- Monte-Carlo-Szenarien statt einer einzigen deterministischen Zukunft,
- explizite Gegnerpolitiken,
- kleine, rundenweise Zustands- und Aktionsräume als sinnvoller Startpunkt,
- wissenschaftliche Beschreibung von Annahmen und Modellgrenzen.

### H2nryHe/Formula-One-Strategy-Sim

**Repository:** <https://github.com/H2nryHe/Formula-One-Strategy-Sim>  
**Stand:** Version 0.8 laut README; historisches Replay  
**Lizenz:** In den geprüften GitHub-Metadaten nicht ausgewiesen

**Was gelöst wird**

- Ingestion historischer Sessions in SQLite,
- kanonischer `RaceState` am Rundenende,
- Rekonstruktion tatsächlicher `PIT`-/`STAY_OUT`-Aktionen,
- deterministische Top-K-Rollout-Empfehlungen,
- strukturierte Begründungen und Gegenfaktuale,
- Offline-Evaluation mit synthetischen Tests und CI.

**Offene Lücken für dieses Projekt**

- ausdrücklich nur historisches Replay, keine Live-Ingestion,
- öffentliche Signale statt Teamtelemetrie und Strategieabsichten,
- vereinfachter Kraftstoff-, Pit-Loss-, Wetter- und Neutralisierungsansatz,
- zunächst unabhängige Gegnerentscheidungen; Cover-Reaktionen, Double Stacks und Pit-Lane-Stau sind geplant,
- keine ausgewiesene Lizenz; deshalb keine Codeübernahme ohne Klärung.

**Was übernommen werden kann**

- Replay zuerst, Live später,
- ein unveränderlicher Informationsstand pro Entscheidungszeitpunkt,
- getrennte Bewertung von „historische Teamaktion getroffen“ und „simulierte Entscheidungsqualität“,
- Top-K-Pläne mit Erwartungswert, Risiko, Reason Codes und Gegenfaktualen,
- reproduzierbare Runs mit Session-ID, Modellversion, Seed und Annahmekonfiguration.

### j5t3313/f1-strategy-simulator

**Repository:** <https://github.com/j5t3313/f1-strategy-simulator>  
**Status:** archiviert  
**Lizenz:** MIT laut GitHub-Metadaten

**Was gelöst wird**

- FastF1-Daten aus 2024 als Grundlage für Reifenmodelle,
- kraftstoffkorrigierte Rundenzeiten,
- lineare, reifenmischungsspezifische Degradation mit bayesianischer Parameterschätzung,
- Monte-Carlo-Vergleich vorgegebener Strategien,
- Konfidenzintervalle, Risikokennzahlen und interaktive Visualisierung.

**Offene Lücken für dieses Projekt**

- Annahme freier Fahrt,
- keine Nassreifen und kein dynamisches Wetter,
- keine Safety-Car-, VSC- oder Red-Flag-Phasen,
- keine Reaktionen anderer Fahrer,
- keine rundenweise Rekalibrierung und Neuplanung,
- lineare Degradation kann thermische Effekte und einen Reifen-Cliff verfehlen.

**Was übernommen werden kann**

- probabilistische statt punktförmige Degradationsparameter,
- Unsicherheitsintervalle je Strategie,
- Fallback-Parameter bei dünner Datenlage,
- Trennung von Datenaufbereitung, Modellierung, Simulation und Präsentation.

### rembertdesigns/pit-stop-simulator

**Repository:** <https://github.com/rembertdesigns/pit-stop-simulator>  
**Ansatz:** Gymnasium, Q-Learning, PPO, Random Forest und Streamlit

**Was gelöst wird**

- synthetische Simulation von Reifenverschleiß, Kraftstoff, Wetter, Verkehr, Safety Car und VSC,
- diskrete Aktionen für Weiterfahren oder Boxenstopp mit Reifenwahl,
- Vergleich von tabellarischem Q-Learning und PPO,
- interaktive Szenarien und statistische Wiederholungen.

**Offene Lücken für dieses Projekt**

- Training des Rundenzeitmodells auf eigenen Simulationslogs statt nachgewiesener historischer F1-Evaluation,
- reale F1-Datenintegration wird als zukünftige Erweiterung genannt,
- 20-Fahrzeug-Interaktion, dynamische Neuplanung und Undercut-/Overcut-Erkennung waren beim Prüfstand nicht umgesetzt,
- dokumentierte Leistungswerte beziehen sich auf die eigene Simulationsumgebung und beweisen keine Güte bei realen Rennen,
- die README bezeichnet das Projekt als MIT-lizenziert, GitHub erkannte beim Prüfstand jedoch keine Lizenz; Wiederverwendung muss vorab geprüft werden.

**Was übernommen werden kann**

- Gymnasium-kompatible Schnittstelle für spätere RL-Experimente,
- zunächst kleiner diskreter Aktionsraum,
- klare Trennung von Simulationsumgebung und Agent,
- RL erst gegen fachlich sinnvolle Heuristiken und Rollout-Suche vergleichen.

### Weitere kleinere Projekte

- **mukundkk/F1StrategySimulator:** Nutzt die frühere Ergast-API für eine einfache F1-Rennsimulation. Nützlich als Anschauung, aber fachlich und technisch deutlich schmaler als die TUM-Lösung; zudem keine ausgewiesene Lizenz.
- Zahlreiche GitHub-Notebooks optimieren eine einzelne Stopprunde, nutzen genetische Algorithmen oder visualisieren Reifenstints. Meist fehlen mehrere Saisons, Punkt-in-Zeit-Evaluation, Unsicherheitskalibrierung, Gegnerreaktionen und Tests. Sterne oder die Bezeichnung „AI“ sind deshalb kein Qualitätskriterium.

## Qualifying-Prediction

### aes21/laps-of-judgement

**Repository:** <https://github.com/aes21/laps-of-judgement>  
**Lizenz:** MIT

Der Ansatz verwendet FastF1-Trainingsdaten für eine bayesianisch-hierarchische Vorhersage von Qualifying-Zeiten und Positionen. Repräsentative Runden werden unter anderem nach grüner Strecke, weicher Mischung, frischen Reifen und kurzen Stints gefiltert. Konstrukteur- und Fahrereffekte sind hierarchisch modelliert; Streckenentwicklung fließt ein. Posterior-Simulationen liefern Wahrscheinlichkeiten und kennzeichnen Fahrer mit zu wenig Trainingsdaten.

**Lernwert:** Das ist für die Qualifying-Idee ein stärkerer Ausgangspunkt als ein gewöhnlicher Regressor, weil Unsicherheit, partielle Pooling-Effekte und Datenknappheit sichtbar werden.

**Verbleibende Lücken:** Eine einzelne aktuelle Trainingssession kann weiterhin durch Programme, Benzinlast, Verkehr und nicht repräsentative Runs verzerrt sein. Für eine belastbare Projektarbeit wären Walk-forward-Backtests über komplette Saisons, Kalibrierung der Q3-Wahrscheinlichkeiten sowie Benchmarks gegen einfache Baselines nötig.

### DerHefi/F1_Quali_Prediction

**Repository:** <https://github.com/DerHefi/F1_Quali_Prediction>  
**Lizenz:** nicht ausgewiesen

Das Projekt vergleicht lineare, quadratische, kubische und MLP-Regressionsmodelle auf FastF1-Daten. Trainiert wird auf 2018 bis 2020, getestet auf 2021. Zusätzlich werden Genauigkeit und Erklärbarkeit den damaligen AWS-Vorhersagen gegenübergestellt.

**Lernwert:** Zeitlich getrennte Testsaisons, mehrere einfache Baselines und Erklärbarkeit sind wichtiger als ein unnötig komplexes Modell.

**Verbleibende Lücken:** Der Daten- und Regelstand ist alt, die Ausführung notebookzentriert und die Abhängigkeiten sind nicht vollständig reproduzierbar beschrieben. Eine Lizenz ist nicht ausgewiesen.

### AWS/F1 Insights

F1 Insights zeigt unter anderem eine voraussichtliche K.-o.-Zeit im Qualifying sowie Rennstrategie-Grafiken. Laut AWS werden F1-Sensordaten mit langjährigen historischen Daten kombiniert. Die Lösung bestätigt die fachliche Relevanz, ist aber kein reproduzierbarer Benchmark: Code, Features, Labels, Modelle und vollständige Fehlermaße sind nicht öffentlich.

**Bewertung der Fragestellung:** Ein Qualifying Predictor ist mit den vorhandenen Quellen sehr gut umsetzbar, aber die öffentliche Konkurrenz ist relativ dicht. Wissenschaftlicher Mehrwert entsteht nur durch probabilistische, kalibrierte Q3-/Positionswahrscheinlichkeiten, robuste Point-in-Time-Features und konsequente Saison-für-Saison-Backtests.

## Geschlossene operative Produkte

### Catapult RaceWatch

**Produktseite:** <https://www.catapult.com/solutions/racewatch>

RaceWatch kombiniert Timing, GPS, Telemetrie, Race Control, Wetter, Teamfunk und Live-Video. Beschrieben werden laufende Modelle für Fahrerleistung, Reifendegradation, Verkehr und Boxenstoppplanung sowie Echtzeitwarnungen.

**Was es voraussichtlich besser löst**

- zuverlässige Integration vieler Live-Quellen,
- team- und serienbezogene Anpassung,
- operative Dashboards für Boxenmauer, Garage und Fabrik,
- Daten, die öffentlichen Projekten teilweise nicht zur Verfügung stehen.

**Was offen bleibt**

- keine öffentliche Implementierung,
- keine reproduzierbaren Modell- oder Evaluationsdetails,
- kein frei zugänglicher Datensatz für einen fairen Vergleich,
- Preis und Lizenz hängen von einem kommerziellen Angebot ab.

### F1 Insights powered by AWS

**Produktseite:** <https://aws.amazon.com/f1/>

Öffentlich beschrieben sind unter anderem `Battle Forecast`, `Pit Strategy Battle`, `Pit Window`, `Predicted Pit Stop Strategy` und `Undercut Threat`. Die Grafiken erklären Strategie für Zuschauer und bewerten Veränderungen während des Rennens.

**Abgrenzung:** F1 Insights ist ein Beleg für Nutzen und Darstellungsformen, aber weder ein offenes Konkurrenzsystem noch ein geeigneter Ground-Truth-Benchmark. Das Projekt sollte keine Gleichwertigkeit mit F1-internen Sensordaten behaupten.

## Dateninfrastruktur, aber keine Strategieprodukte

| Projekt | Nutzen für dieses Projekt | Was es nicht löst |
|---|---|---|
| [FastF1](https://github.com/theOehrly/Fast-F1) | Komfortable historische Sessions, Runden, Stints, Wetter und Telemetrie | Keine Strategieentscheidung und keine reguläre Live-Verarbeitung |
| [OpenF1](https://github.com/br-g/openf1) | HTTP-, WebSocket- und MQTT-Zugang zu Timing-, Fahrzeug-, Wetter- und Ereignisdaten | Kein offenes Strategie- oder Reifenmodell |
| [Jolpica F1](https://github.com/jolpica/jolpica-f1) | Langjährige Kalender-, Ergebnis- und Standings-Daten; Ergast-Nachfolger | Zu geringe Granularität für dynamische Rennstrategie |
| [F1DB](https://github.com/f1db/f1db) | Versionierte historische Stammdaten und Ergebnisse | Keine hochfrequenten Zustände oder Strategieoptimierung |

Für den Simulator sind FastF1 und OpenF1 deshalb Eingangsquellen. Jolpica und F1DB eignen sich für langfristige Baselines, Fahrer-/Teamhistorie und die 2026-Auswertung.

## Deckt ein bestehendes Produkt die Forschungsfrage vollständig ab?

### Öffentliche Projekte

**Nein.** Die Teilprobleme sind verteilt:

- TUM löst Simulation, Gegner und probabilistische Szenarien am umfassendsten.
- Formula-One-Strategy-Sim löst Replay, Informationsgrenzen, Erklärungen und Offline-Evaluation konzeptionell am besten.
- Der archivierte F1 Strategy Simulator zeigt eine brauchbare bayesianische Reifenmodellierung mit realen FastF1-Daten.
- RL-Demos zeigen Agentenschnittstellen, validieren ihre Ergebnisse aber überwiegend in der eigenen synthetischen Welt.

Es fehlt die nachgewiesene Kombination aus:

1. realen öffentlichen Daten,
2. strengem Informationsstand am jeweiligen Rundenende,
3. laufender Aktualisierung von Pace und Degradation,
4. Unsicherheit für Wetter und Neutralisierungsdauer,
5. Gegner- und Verkehrseffekten,
6. Neuplanung nach jeder Runde,
7. mehreren Rennen umfassender, leakage-freier Evaluation.

### Geschlossene Produkte

**Operativ wahrscheinlich weitgehend, wissenschaftlich nicht überprüfbar.** RaceWatch und F1/AWS verfügen über integrierte Live-Systeme und reichhaltigere Daten. Wegen fehlender Offenheit kann weder überprüft werden, welche Modelle welche Situationen lösen, noch ob eine Empfehlung unter denselben öffentlichen Informationen besser als eine Baseline ist.

## Vergleich der drei Projektideen

Bewertung von 1 (schwach) bis 5 (stark). Die Gesamtnote nutzt: Datenpassung 25 %, offene Forschungslücke 25 %, Evaluierbarkeit 20 %, technische Machbarkeit 15 %, Differenzierung 15 %.

| Idee | Datenpassung | Forschungslücke | Evaluierbarkeit | Machbarkeit | Differenzierung | Gewichtet |
|---|---:|---:|---:|---:|---:|---:|
| Dynamische Rennstrategie | 4 | 5 | 4 | 3 | 5 | **4,25** |
| Qualifying Predictor | 4 | 2 | 5 | 4 | 2 | **3,40** |
| Bewertung 2026 | 3 | 4 | 2 | 4 | 4 | **3,35** |

### Empfehlung

Der **dynamische Rennstrategie-Simulator** bleibt die stärkste Hauptfragestellung. Er nutzt fast alle bereits geprüften Datendomänen und besitzt trotz vorhandener Vorarbeiten eine klar benennbare offene Lücke. Der Qualifying Predictor eignet sich als kleineres Vergleichs- oder Nebenmodell. Die 2026-Bewertung eignet sich als eigenständige deskriptive Studie, aber noch nicht für eine starke kausale Aussage.

## Konkretisierte Forschungsfrage

> Wie stark reduziert eine nach jedem Rundenende mit ausschließlich bis dahin beobachteten öffentlichen Daten aktualisierte Reifen- und Boxenstoppstrategie die erwartete verbleibende Rennzeit gegenüber einem vor dem Start festgelegten Strategieplan?

Diese Formulierung präzisiert die ursprüngliche Frage:

- **Entscheidungszeitpunkt:** Ende jeder Runde,
- **Information:** nur bis zu diesem Zeitpunkt bekannte Daten,
- **Aktionen:** draußen bleiben oder in einem definierten Fenster auf eine zulässige Mischung wechseln,
- **Dynamische Faktoren:** beobachtete Pace, geschätzte Reifendegradation, Wetterzustand, Verkehr und SC/VSC,
- **Primärziel:** erwartete verbleibende Rennzeit,
- **Sekundärziel:** erwartete Position beziehungsweise Punkte,
- **Vergleich:** fixer Vor-Renn-Plan und einfache regelbasierte Strategie.

Die historische Teamaktion ist **kein Beweis für die optimale Aktion**. Teams verfügen über private Daten und andere Ziele. Deshalb müssen Verhaltensähnlichkeit und simulierte Entscheidungsqualität getrennt berichtet werden.

## Empfohlener MVP

### 1. Historisches Point-in-Time-Replay

Eine Session wird chronologisch abgespielt. Für Runde `L` darf der Zustand ausschließlich Informationen mit Zeitstempel bis zum Ende dieser Runde enthalten. Spätere Stintlänge, tatsächliches Wetter und spätere Safety Cars dürfen nicht als Features einfließen.

### 2. Kanonischer Race State

Mindestens:

- Runde, Restdistanz, Position und Abstände,
- aktuelle Mischung, Reifenalter und Stint,
- robuste Pace der letzten gültigen Runden,
- Track-Status und bekannte Race-Control-Ereignisse,
- Wetterzustand und kurzfristiger Trend,
- geschätzter normaler und neutralisierter Pit Loss,
- Verkehr nach einem hypothetischen Stopp,
- Unsicherheit der Pace- und Degradationsparameter.

### 3. Basismodelle vor Machine Learning

- robuste lineare oder stückweise Degradation je Mischung,
- fahrer-/teamspezifische Basispace mit partieller Pooling-Option,
- streckenspezifischer Pit Loss,
- einfache Regeln für SC/VSC,
- Verkehr als Funktion erwarteter Rejoin-Position und Abstände.

Erst wenn diese Modelle in Backtests stabil sind, sollten Gradient Boosting, Zustandsraummodelle oder RL ergänzt werden.

### 4. Rollout und Neuplanung

Pro Runde werden `STAY_OUT` und zulässige Stopps innerhalb der nächsten Runden simuliert. Monte-Carlo-Szenarien variieren Pace, Degradation, Wetterwechsel, Neutralisierungsdauer und Gegneraktionen. Nach der nächsten beobachteten Runde wird der Zustand aktualisiert und neu geplant.

Ausgabe pro Alternative:

- erwartete verbleibende Rennzeit,
- Median und Streuung,
- ungünstiges Quantil beziehungsweise CVaR,
- erwartete Rejoin-Position,
- Wahrscheinlichkeit eines Vorteils gegenüber `STAY_OUT`,
- maschinenlesbare Begründungen wie `TYRE_CLIFF`, `SC_WINDOW`, `RAIN_RISK`, `UNDERCUT_THREAT` und `TRAFFIC_PENALTY`.

### 5. Leakage-freie Evaluation

Mindestens vier Baselines:

1. fixer Vor-Renn-Plan,
2. einfache Reifenalter-/Pace-Schwelle,
3. historisch tatsächlich ausgeführte Aktion als Verhaltensvergleich,
4. Hindsight-Oracle mit vollständigem Rennwissen nur als obere Schranke.

Geeignete Kennzahlen:

- Regret gegenüber dem Hindsight-Oracle,
- simulierte Zeitdifferenz zur festen Strategie,
- Stabilität der Empfehlung über benachbarte Runden,
- Kalibrierung der angegebenen Gewinnwahrscheinlichkeit,
- Güte der Pace- und Degradationsprognose,
- getrennte Ergebnisse für normale, SC/VSC- und Wetterrennen.

Die Aufteilung erfolgt zeitlich nach Rennen oder Saisons, nicht zufällig nach einzelnen Runden. Sonst gelangen nahezu identische Runden desselben Rennens in Training und Test.

## Bewertung der Fahrzeuggeneration 2026

Die Frage „Hat 2026 das Racing verbessert?“ muss operationalisiert werden. Geeignete öffentliche Kennzahlen sind:

- Überholmanöver auf der Strecke, soweit zuverlässig klassifizierbar,
- Führungswechsel ohne reine Boxenstoppzyklen,
- Anteil der Rennzeit in engem Abstand,
- Abstände im Ziel und pace-bereinigte Feldspreizung,
- Anzahl unterschiedlicher Sieger, Podiumsteams und Konstrukteurskonzentration,
- Qualifying- und Rennpace-Abstände zwischen Teams,
- Positionsgewinne und -verluste ohne Ausfälle oder Strafen.

Ein einfacher Vergleich der Anzahl von Überholmanövern reicht nicht. Zu kontrollieren sind mindestens Strecke, Regen, Safety Car, rote Flaggen, Sprintformat, Ausfälle, Startplatzverteilung und Regeländerungen am Überholsystem. Der OpenF1-Endpunkt `overtakes` kann unvollständig sein und auch strategische Positionswechsel enthalten.

**Stand Juli 2026:** Eine Zwischenbilanz ist möglich. Eine robuste Aussage über die gesamte Fahrzeuggeneration benötigt mindestens die vollständige Saison und idealerweise mehrere Jahre. Da alle Teams gleichzeitig behandelt werden, ist eine kausale Wirkung der Regeln schwer von Reifen, Streckenkalender, Teamstärke und Saisonverlauf zu trennen.

## Umsetzungsreihenfolge

1. Historische Rennen in einen gemeinsamen, zeitlich sauberen Race State überführen.
2. Fixe Strategie und fachliche Heuristik als Baselines implementieren.
3. Pace-, Degradations- und Pit-Loss-Modelle mit Unsicherheit schätzen.
4. Rollout-Suche und rundenweise Neuplanung ergänzen.
5. Mehrere Trockenrennen mit unterschiedlichen Streckencharakteristiken backtesten.
6. SC/VSC und Wetter getrennt hinzufügen und auswerten.
7. Gegnerreaktionen, Positionsziel und Teamstrategie erst danach modellieren.
8. Echte Live-Ingestion nur nach erfolgreichem historischem Replay angehen.

## Quellen

### Repositories und Datenprojekte

- TUMFTM Race Simulation: <https://github.com/TUMFTM/race-simulation>
- Formula-One-Strategy-Sim: <https://github.com/H2nryHe/Formula-One-Strategy-Sim>
- F1 Strategy Simulator 2025: <https://github.com/j5t3313/f1-strategy-simulator>
- F1 Pit Stop Strategy Simulator: <https://github.com/rembertdesigns/pit-stop-simulator>
- Einfacher Ergast-basierter Simulator: <https://github.com/mukundkk/F1StrategySimulator>
- Laps of Judgement: <https://github.com/aes21/laps-of-judgement>
- F1 Qualifying Prediction: <https://github.com/DerHefi/F1_Quali_Prediction>
- FastF1: <https://github.com/theOehrly/Fast-F1>
- OpenF1: <https://github.com/br-g/openf1>
- Jolpica F1: <https://github.com/jolpica/jolpica-f1>
- F1DB: <https://github.com/f1db/f1db>

### Wissenschaftliche Arbeiten

- Heilmeier, Graf und Lienkamp (2018), *A Race Simulation for Strategy Decisions in Circuit Motorsports*: <https://doi.org/10.1109/ITSC.2018.8570012>
- Heilmeier et al. (2020), *Application of Monte Carlo Methods to Consider Probabilistic Effects in a Race Simulation for Circuit Motorsport*: <https://doi.org/10.3390/app10124229>
- Heilmeier et al. (2020), *Virtual Strategy Engineer: Using Artificial Neural Networks for Making Race Strategy Decisions in Circuit Motorsport*: <https://doi.org/10.3390/app10217805>

### Produkte

- Catapult RaceWatch: <https://www.catapult.com/solutions/racewatch>
- F1 Insights powered by AWS: <https://aws.amazon.com/f1/>

Repository-Status und Produktumfang wurden am 21. Juli 2026 geprüft. Sie können sich nach diesem Datum ändern.

