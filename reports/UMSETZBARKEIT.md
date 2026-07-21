# Machbarkeitsbewertung der F1-Datenquellen

**Stand:** 21. Juli 2026  
**Bewertete Quellen:** FastF1 3.8.3 und OpenF1 v1

## Kurzfazit

Die geplanten Analysen sind **für historische Sessions grundsätzlich umsetzbar**. Rundenzeiten, Sektoren, Topspeed, Fahrzeugtelemetrie, Reifenstints, Wetter und Rennereignisse reichen für Vergleiche wie „hoher Topspeed, aber langsame Runde“ aus.

- **FastF1** ist besonders geeignet für nachträgliche Analysen in Python, weil Session-, Runden- und Telemetriedaten direkt als pandas-kompatible Objekte bereitstehen.
- **OpenF1** ist besonders geeignet für API-basierte Datenpipelines, aktuelle Rennwochenenden und – mit kostenpflichtigem Zugang – Live-Dashboards.
- **Für Live-Anwendungen ist die kostenlose OpenF1-Stufe nicht ausreichend.** FastF1 besitzt zwar einen Live-Timing-Recorder, bietet damit aber keine reguläre Echtzeitverarbeitung während der Session.
- **Empfehlung:** OpenF1 für aktuelle beziehungsweise live eingehende Daten verwenden und FastF1 später zur vertieften Analyse und Gegenprüfung ergänzen.

**Beide Projekte sind inoffiziell:** Weder FastF1 noch OpenF1 ist mit Formula 1, FIA oder Formula One Management verbunden oder von diesen als offizielles Datenprodukt lizenziert. Das bedeutet nicht, dass die Daten frei erfunden oder grundsätzlich unzuverlässig sind. Beide Projekte bereiten Daten auf, die zumindest teilweise aus öffentlich erreichbaren F1-Timing- und Ergebnisquellen stammen. Inoffiziell sind die Zugriffswege, Bibliotheken und APIs.

Für dieses Projekt hat das konkrete Folgen:

- Es gibt keine garantierte Verfügbarkeit, Vollständigkeit oder Aktualisierungszeit (SLA).
- Änderungen an zugrunde liegenden F1-Endpunkten können beide Dienste kurzfristig beeinträchtigen.
- Leere Antworten, fehlende Felder und Schemaänderungen müssen durch Validierung, Retries und Caching abgefangen werden.
- Für wissenschaftliche, persönliche oder prototypische Analysen sind die Quellen gut nutzbar. Eine kommerzielle Veröffentlichung benötigt dagegen eine separate Prüfung von Lizenz-, Marken- und Datenrechten.
- Kritische Ergebnisse sollten möglichst mit einer zweiten Quelle oder offiziellen veröffentlichten Resultaten plausibilisiert werden.

## Was wurde in diesem Projekt praktisch geprüft?

Die vorhandenen Skripte prüfen bisher Monza 2024:

| Skript | Geprüfter Fall | Ergebnis |
|---|---|---|
| `src/fastf1_probe.py` | FastF1, Qualifying | Schnellste Runde und Telemetrie mit Distanz, Geschwindigkeit, Gas, Bremse, Gang und Drehzahl konnten geladen und als Parquet gespeichert werden. |
| `src/openf1_probe.py` | OpenF1, Rennen | Sessions, Fahrer, Runden, Wetter, Boxenstopps, Rennkontrolle und Fahrzeugdaten konnten abgefragt werden. Persistiert werden Runden, Wetter und exemplarisch Fahrzeugdaten von Fahrer 1. |
| `src/compare_sources.py` | FastF1, Rennen | Rundenanzahl sowie beste und durchschnittliche Rundenzeit je Fahrer konnten aggregiert werden. |

Damit ist der technische Grundweg **API/Bibliothek → pandas DataFrame → Parquet → Analyse** nachgewiesen. Noch nicht implementiert sind eine parametrisierbare Eventauswahl, automatische Aktualisierung, Retries, ein echter Live-Stream und ein direkter feldweiser Vergleich beider Quellen.

### Warum wurde das Qualifying nur mit FastF1 geprüft?

Das ist **keine Einschränkung der Schnittstellen**, sondern eine Entscheidung des Prototyps:

- `fastf1_probe.py` verwendet das Qualifying, weil eine einzelne schnellste Runde besonders einfach für einen Telemetrie- und Geschwindigkeitsvergleich validiert werden kann.
- `openf1_probe.py` verwendet das Rennen, weil dort zusätzlich Runden, Wetter, Boxenstopps und Rennkontrollmeldungen gemeinsam geprüft werden können.
- FastF1 kann grundsätzlich auch Rennsessions laden; das macht bereits `compare_sources.py`.
- OpenF1 kann grundsätzlich auch Trainings- und Qualifyingsessions liefern.

Der aktuelle Code vergleicht daher noch nicht dieselbe Session zwischen beiden Quellen. Für einen belastbaren Quellenvergleich sollten beide Anbieter für **dasselbe Event, dieselbe Session, dieselben Fahrer und dieselben Runden** abgefragt werden.

## Was liefert FastF1?

FastF1 ist eine Python-Bibliothek und keine klassische REST-Schnittstelle. Die Daten werden überwiegend als erweiterte pandas-Objekte bereitgestellt.

Kurz zusammengefasst liefert FastF1:

- **Eventkalender und Sessions:** Grand Prix, Strecke, Land, Sessiontypen und geplante Startzeiten.
- **Sessionergebnisse und Fahrer:** Positionen, Fahrer- und Teaminformationen sowie Ergebnisdaten, sofern upstream verfügbar.
- **Runden:** Runden- und Sektorzeiten, Rundenstatus, Speed-Trap-Werte, Boxenein- und -ausfahrten und gültige/ungültige Runden.
- **Reifen und Stints:** Mischung, Reifenalter, Stintverlauf und Strategieinformationen.
- **Fahrzeugtelemetrie:** Zeit, Distanz, Geschwindigkeit, Gas, Bremse, Gang, Drehzahl und DRS; zusätzlich Positionsdaten.
- **Wetter:** Luft- und Streckentemperatur, Feuchtigkeit, Luftdruck, Regen und Wind.
- **Session- und Streckenstatus:** Flaggen-/Track-Status und Rennkontrollmeldungen, soweit vorhanden.
- **Historische Ergebnisdaten:** Über die Ergast-kompatible Jolpica-F1-Anbindung auch ältere Kalender- und Ergebnisdaten, allerdings mit geringerem Detailgrad.

### Stärken von FastF1

- Sehr komfortabel für Python- und pandas-Analysen.
- Gute Verknüpfung von Runde, Fahrer, Reifen und Telemetrie.
- Interpolierte Distanz ermöglicht Telemetrievergleiche entlang einer Runde.
- Lokaler Cache reduziert wiederholte Downloads.
- Für Visualisierungen und explorative Analysen gut geeignet.

## Was liefert OpenF1?

OpenF1 stellt die Daten als REST-API in **JSON** oder **CSV** bereit. Stand Juli 2026 sind 18 Endpunkte dokumentiert:

| Endpunkt | Inhalt |
|---|---|
| `car_data` | Geschwindigkeit, Gas, Bremse, Gang, Drehzahl und DRS je Fahrzeug, ungefähr 3,7 Messungen pro Sekunde. |
| `championship_drivers` | Fahrer-WM-Positionen und Punkte vor beziehungsweise während/nach einem Rennen; Beta. |
| `championship_teams` | Konstrukteurs-WM-Positionen und Punkte; Beta. |
| `drivers` | Fahrername, Nummer, Kürzel, Team, Teamfarbe und Bild-URL. |
| `intervals` | Abstand zum Führenden und zum vorausfahrenden Auto, im Rennen ungefähr alle vier Sekunden. |
| `laps` | Runden- und Sektorzeiten, Zwischenpunkt- und Speed-Trap-Geschwindigkeiten sowie Mini-Sektoren. |
| `location` | Ungefähre x-/y-/z-Position des Fahrzeugs, ungefähr 3,7 Messungen pro Sekunde. |
| `meetings` | Informationen zum gesamten Grand-Prix- oder Testwochenende. |
| `overtakes` | Erkannte Positionswechsel; nur Rennen und möglicherweise unvollständig. |
| `pit` | Boxengassendauer, Runde und – für neuere Events – Standzeit. |
| `position` | Position eines Fahrers im Verlauf einer Session. |
| `race_control` | Flaggen, Safety Car, Sessionstatus, Vorfälle und Meldungen der Rennleitung. |
| `sessions` | Training, Qualifying, Sprint und Rennen mit Zeiten und Session-IDs. |
| `session_result` | Endergebnis, Rundenanzahl, Abstände sowie DNS/DNF/DSQ; wenige Minuten nach Veröffentlichung des offiziellen Ergebnisses. |
| `starting_grid` | Startaufstellung und zugehörige Qualifying-Zeit. |
| `stints` | Reifenmischung, Start-/Endrunde und Reifenalter je Stint. |
| `team_radio` | URLs zu einer begrenzten Auswahl veröffentlichter Funksprüche. |
| `weather` | Luft-/Streckentemperatur, Feuchtigkeit, Druck, Regen und Wind; ungefähr minütlich. |

Die OpenF1-Abfragen lassen sich serverseitig nach fast allen einzelnen Attributen und Zeiträumen filtern. Das ist für schlanke Datenpipelines hilfreich, insbesondere bei großen Telemetriemengen.

## Aktualität und Live-Fähigkeit

### OpenF1

Nach aktueller Anbieterinformation gilt:

- Historische Daten sind **ab der Saison 2023** verfügbar.
- Historische Daten sind ohne Anmeldung und ohne API-Key kostenlos abrufbar.
- Eine Session gilt von **30 Minuten vor dem Start bis 30 Minuten nach dem Ende** als live.
- Während dieses Fensters ist ein **kostenpflichtiger Zugang** erforderlich.
- Live-Daten werden über REST, MQTT und WebSocket angeboten.
- Der Anbieter nennt für Live-Daten eine typische Verzögerung von ungefähr **drei Sekunden**.
- Nach dem Live-Fenster werden die Daten als historische Daten eingestuft und können kostenlos abgefragt werden.
- `session_result` und `starting_grid` erscheinen laut Dokumentation wenige Minuten nach Veröffentlichung der offiziellen Ergebnisse.

Für ein kostenloses Analyseprojekt bedeutet das: Eine Session sollte normalerweise kurz nach Ende des Live-Fensters historisch abrufbar sein. Eine garantierte Verfügbarkeitszeit oder Vollständigkeit wird daraus jedoch nicht abgeleitet; die Pipeline muss leere oder verspätete Antworten tolerieren.

### FastF1

FastF1 lädt reguläre Sessiondaten nachträglich aus zugrunde liegenden F1-/Ergebnisquellen. Es gibt **keine zugesicherte feste Verzögerung**, nach der eine gerade beendete Session vollständig über `Session.load()` verfügbar ist.

Der vorhandene `fastf1.livetiming.SignalRClient` kann einen Live-Timing-Stream während einer Session in eine Datei aufzeichnen. Laut FastF1-Dokumentation ist damit aber **keine Echtzeitverarbeitung während der Session** vorgesehen; die aufgezeichneten Daten werden anschließend geladen und verarbeitet. Das aktuelle Projekt verwendet diesen Recorder nicht.

Für ältere Daten gilt außerdem:

- FastF1 stellt eigene detaillierte Eventkalender für 2018 und später bereit.
- Vor 2018 werden Kalenderdaten über die Ergast-kompatible Ergebnisquelle aufgebaut und sind eingeschränkt.
- Detaillierte Timing- und Telemetriedaten hängen generell davon ab, was die jeweilige Upstream-Quelle für eine Session veröffentlicht hat.

## Kosten und Nutzungsmodelle

Die Annahme „OpenF1 ist vollständig kostenlos und FastF1 kostet“ ist nicht korrekt. Stand 21. Juli 2026 gilt vielmehr:

| Quelle / Nutzung | Preis | Enthalten / Einschränkung |
|---|---:|---|
| **FastF1** | **0 €** | Bibliothek und regulärer Datenabruf sind kostenlos; kein API-Key und kein Abonnement erforderlich. |
| FastF1-Sponsoring | freiwillig | GitHub Sponsors oder „Buy Me a Coffee“ unterstützen nur die Entwicklung und schalten keine zusätzlichen Datenfunktionen frei. |
| **OpenF1 Community** | **0 €** | Alle 18 Endpunkte für historische Sessions ab 2023, persönliche Nutzung, 3 Requests/s und 30 Requests/min, kein API-Key. |
| **OpenF1 Sponsor** | **9,90 € pro Monat** | Historische und Live-Daten für persönliche Nutzung, REST/MQTT/WebSocket, 6 Requests/s und 60 Requests/min sowie laut Anbieter bis zu 10 gleichzeitige MQTT-/WebSocket-Verbindungen. |
| Kommerzielle Nutzung | separat zu klären | Der OpenF1-Sponsorpreis ist nicht automatisch eine kommerzielle Datenlizenz. OpenF1 fordert für andere als persönliche, Bildungs-, Forschungs- und nichtkommerzielle Fan-Anwendungen zur Kontaktaufnahme auf. Auch bei FastF1 deckt die MIT-Lizenz der Software nicht automatisch Rechte an F1-Daten und Marken ab. |

FastF1 selbst hat somit **keine kostenpflichtige Datenstufe**. Mögliche Kosten entstehen nur indirekt, etwa für eigenen Serverbetrieb, Speicher, Datenbank, Monitoring oder freiwillige Unterstützung des Projekts. Bei OpenF1 wird ein Abonnement nur benötigt, wenn während des Live-Fensters zugegriffen oder das höhere Kontingent genutzt werden soll. Für die nachträgliche persönliche Analyse historischer Sessions reicht grundsätzlich die kostenlose Community-Stufe.

## Konkreter Aktualitätstest: Belgien 2026

Der Belgische Grand Prix fand vom **17. bis 19. Juli 2026** in Spa-Francorchamps statt. Am **21. Juli 2026** wurde der Datenzugriff mit der Projektumgebung geprüft.

### OpenF1-Ergebnis

OpenF1 lieferte das Rennwochenende und alle fünf Sessions. Für das Rennen mit `session_key=11334` waren über den kostenlosen historischen Endpunkt unter anderem verfügbar:

- 22 Einträge im Sessionergebnis,
- 44 Runden für den exemplarisch abgefragten Fahrer 1,
- 146 Wettermessungen,
- Fahrzeugtelemetrie mit Bremse, DRS, Gang, Drehzahl, Geschwindigkeit und Gas.

**Bewertung:** Das Rennen vom vorherigen Wochenende war zwei Tage später bereits für eine aktuelle nachträgliche Analyse verfügbar. Das beweist den geprüften Stand, ist aber keine SLA für zukünftige Events.

### FastF1-Ergebnis

FastF1 3.8.3 kannte den Termin und Namen des Belgien-GP 2026. Der Aufruf von `Session.load()` konnte am 21. Juli jedoch noch keine Fahrer-, Ergebnis-, Runden- oder Timingdaten laden; die Session enthielt danach keine Fahrer und keine Runden.

**Bewertung:** FastF1 eignet sich nicht als einzige Quelle, wenn ein gerade beendetes Rennen zu einem festen frühen Zeitpunkt ausgewertet werden muss. Ein späterer erneuter Abruf kann erfolgreich sein, der Zeitpunkt ist aber nicht garantiert. Für diesen Anwendungsfall sollte OpenF1 primär und FastF1 später ergänzend eingesetzt werden.

## Limits und Einschränkungen

### OpenF1

- **Historische Reichweite:** Detaillierte OpenF1-Daten erst ab 2023.
- **Kostenloses Kontingent:** maximal 3 Requests pro Sekunde und 30 Requests pro Minute.
- **Bezahltes Sponsor-Kontingent:** maximal 6 Requests pro Sekunde und 60 Requests pro Minute; Live-Zugang eingeschlossen.
- **Live nicht kostenlos:** Im Live-Fenster ist Authentifizierung beziehungsweise ein Abonnement nötig.
- **Sampling:** Fahrzeug- und Positionsdaten mit ungefähr 3,7 Hz sind für Analyse und Visualisierung brauchbar, aber keine hochfrequente Ingenieurtelemetrie.
- **Position nur näherungsweise:** Keine genaue seitliche Position auf der Strecke; der Koordinatenursprung ist nicht eindeutig georeferenziert.
- **Mini-Sektoren:** Nicht im Rennen verfügbar und nicht immer deckungsgleich mit der TV-Anzeige.
- **Überholmanöver:** Können unvollständig sein und umfassen auch Positionswechsel durch Stopps oder Strafen.
- **Boxenstandzeit:** `stop_duration` ist erst ab dem USA-GP 2024 verfügbar.
- **Teamradio:** Immer nur eine Auswahl; seit 2026 wird für die meisten Events laut Anbieter gar kein Radio mehr veröffentlicht.
- **Schemaänderungen:** Beta-Endpunkte und als veraltet markierte Felder können sich ändern; beispielsweise soll `pit_duration` nach 2026 entfallen.
- **Lizenz/Nutzung:** OpenF1 bezeichnet die freie Nutzung als persönlich und die Daten als für nichtkommerzielle Analyse/Fan-Nutzung bestimmt. Für kommerzielle Nutzung müssen Lizenz und Rechte separat geklärt werden.

### FastF1

- **Keine verlässliche Sofortverfügbarkeit:** Gerade beendete Sessions können trotz vorhandenem Kalendereintrag noch nicht ladbar sein.
- **Kein reguläres Live-Analyse-API:** Der Live-Timing-Client zeichnet auf, verarbeitet die Daten aber erst anschließend.
- **Upstream-Abhängigkeit:** Fehlende oder geänderte F1-Endpunkte können zu leeren Sessions, fehlenden Spalten oder Ladefehlern führen.
- **Uneinheitliche historische Tiefe:** Ältere Saisons besitzen weniger Details; nicht jede Session enthält vollständige Telemetrie, Wetter- oder Statusdaten.
- **Datenmenge:** Telemetrie aller Fahrer ist groß und benötigt Zeit, Speicher und einen konsequent genutzten Cache.
- **Synchronisation:** Telemetrie und Positionsdaten werden teilweise interpoliert oder auf gemeinsame Zeitachsen gebracht; sie sind keine unveränderten Rohdaten eines Teamsensors.
- **Live-Recorder:** Für einen vollständigen Datensatz müsste die Aufzeichnung schon vor Sessionstart zuverlässig laufen. Das ist betrieblich aufwendiger als ein nachträglicher Abruf.

### Fachliche Grenzen beider Quellen

Mit beiden Quellen lässt sich Fahrzeugperformance nur **beobachten**, nicht vollständig technisch erklären. Nicht oder nicht zuverlässig enthalten sind insbesondere:

- tatsächliche Downforce- und Luftwiderstandswerte,
- Fahrzeugsetup wie Flügelwinkel, Bodenfreiheit oder Fahrwerkseinstellungen,
- Kraftstoffmenge und exakte Fahrzeugmasse,
- Reifendruck und interne Reifentemperaturen,
- detaillierte Hybrid-/Energieabgabe und Motor-Modi,
- Windschatten- und Verkehrseinfluss als fertige Kennzahl,
- vollständige Informationen über Schäden oder technische Probleme.

Die Aussage „Auto A ist wegen des Setups schneller“ kann daher nicht direkt aus den Daten bewiesen werden. Möglich sind belastbare **Korrelationen und Vergleiche**, beispielsweise Topspeed gegen Sektor- und Rundenzeit, sofern Verkehr, Reifen, Wetter, Track-Status und Rundengültigkeit kontrolliert werden.

## Auswahl der analytischen Fragestellung

Neben der ursprünglichen Fahrzeugperformance-Analyse wurden drei mögliche Schwerpunkte bewertet. Details zu öffentlichen Repositories, Forschungsarbeiten und geschlossenen Produkten stehen in der [Konkurrenzanalyse](KONKURRENZANALYSE.md).

| Idee | Datenpassung | Offene Forschungslücke | Evaluierbarkeit | Gesamtbewertung |
|---|---:|---:|---:|---:|
| Dynamische Rennstrategie | hoch | sehr hoch | hoch | **4,25 / 5** |
| Qualifying Predictor | hoch | mittel bis gering | sehr hoch | **3,40 / 5** |
| Bewertung der Fahrzeuggeneration 2026 | mittel | hoch | derzeit eingeschränkt | **3,35 / 5** |

### Priorität: dynamischer Rennstrategie-Simulator

Die favorisierte Forschungsfrage lautet präzisiert:

> Wie stark reduziert eine nach jedem Rundenende mit ausschließlich bis dahin beobachteten öffentlichen Daten aktualisierte Reifen- und Boxenstoppstrategie die erwartete verbleibende Rennzeit gegenüber einem vor dem Start festgelegten Strategieplan?

Die Einschränkung auf den damaligen Informationsstand ist wesentlich. Ein historischer Replay-Simulator darf spätere Safety Cars, Wetterwechsel, Stintlängen oder Rennergebnisse nicht als Eingabe verwenden. Andernfalls würde die Evaluation durch Zukunftswissen verzerrt.

Öffentliche Projekte lösen Teilprobleme wie Monte-Carlo-Rennsimulation, bayesianische Reifenmodelle, Reinforcement Learning oder historische Replays. Eine nachgewiesene offene End-to-End-Lösung aus öffentlichen F1-Daten, rundenweiser Zustandsschätzung, Unsicherheitsmodellierung, Gegnerreaktionen, dynamischer Neuplanung und leakage-freiem Backtesting wurde nicht gefunden. Geschlossene Systeme wie Catapult RaceWatch und F1 Insights powered by AWS zeigen die praktische Relevanz, veröffentlichen aber weder Modelle noch vollständige Benchmarks und nutzen teilweise nicht öffentliche Signale.

Der empfohlene MVP ist deshalb **kein echtes Live-System**, sondern ein historisches Point-in-Time-Replay:

1. Zustand am Ende jeder Runde aus bis dahin bekannten Daten rekonstruieren.
2. Pace, Reifendegradation und Pit Loss samt Unsicherheit aktualisieren.
3. `STAY_OUT` und zulässige Stopps per Rollout beziehungsweise Monte Carlo vergleichen.
4. Gegen einen festen Vor-Renn-Plan und eine einfache Heuristik evaluieren.
5. Erst danach Wetter, SC/VSC, Gegnerreaktionen und echte Live-Ingestion ausbauen.

Die historische Teamentscheidung ist dabei kein optimaler Ground Truth: Teams besitzen private Daten und können Position, Punkte oder beide Fahrzeuge statt reiner Rennzeit optimieren. Verhaltensähnlichkeit und simulierte Entscheidungsqualität müssen daher getrennt ausgewiesen werden.

### Einordnung der Alternativen

- **Qualifying Predictor:** Sehr gut umsetzbar und leicht über abgeschlossene Sessions zu testen. Die öffentliche Konkurrenz enthält jedoch bereits klassische und bayesianisch-hierarchische Modelle. Ein neuer Beitrag müsste kalibrierte Q3-/Positionswahrscheinlichkeiten, Point-in-Time-Features und saisonweise Walk-forward-Tests liefern.
- **Fahrzeuggeneration 2026:** Als deskriptive Nebenstudie geeignet. Überholmanöver, Führungswechsel, enge Abstände und Wettbewerbsverteilung können verglichen werden. Eine belastbare kausale Aussage benötigt jedoch mindestens eine vollständige Saison sowie Kontrollen für Strecke, Wetter, Neutralisierungen, Ausfälle und Kalender.

## Einschränkungen des aktuellen Projektcodes

Unabhängig von den Anbietern ist der gegenwärtige Prototyp noch beschränkt:

- Event, Jahr und Session sind auf Monza 2024 fest eingestellt.
- FastF1 untersucht im Probe-Skript nur die schnellste Qualifying-Runde.
- OpenF1 speichert Fahrzeugdaten nur für Fahrer 1.
- FastF1-Qualifying und OpenF1-Rennen sind kein direkter Eins-zu-eins-Vergleich.
- `compare_sources.py` vergleicht derzeit nicht beide Anbieter, sondern plausibilisiert nur FastF1-Rundenzeiten.
- Es fehlen Retries, Backoff für Rate Limits, Vollständigkeitsprüfungen und ein automatisches Nachladen verspäteter Daten.
- Es gibt noch keine Live-Ingestion, Datenbank oder geplante Aktualisierung.

## Empfehlung für die Umsetzung

### Historische Analyse

**Gut umsetzbar.** FastF1 als primäre Analysequelle verwenden und OpenF1 für Gegenprüfung oder fehlende aktuelle Sessions ergänzen. Rohdaten unverändert in `data/raw/` speichern und bereinigte, normalisierte Tabellen nach `data/processed/` schreiben.

Für den Rennstrategie-MVP sollten zunächst mehrere historische Trockenrennen mit unterschiedlichen Streckencharakteristiken als Point-in-Time-Replay ausgewertet werden. Safety-Car- und Wetterrennen werden anschließend als eigene Szenarioklassen ergänzt, damit Modellfehler klar zugeordnet werden können.

### Analyse direkt nach einem Rennen

**Umsetzbar, bevorzugt mit OpenF1.** Nach Ende des OpenF1-Live-Fensters automatisiert abfragen, Ergebnisse auf Vollständigkeit prüfen und bei fehlenden Daten zeitversetzt erneut versuchen. FastF1 später als zweite Quelle nachladen.

### Echtes Live-Dashboard

**Umsetzbar, aber nicht mit dem aktuellen Code und nicht rein kostenlos.** Erforderlich sind mindestens:

1. bezahlter OpenF1-Live-Zugang,
2. WebSocket- oder MQTT-Consumer,
3. Zwischenspeicherung und Wiederverbindungslogik,
4. Rate-Limit- und Fehlerbehandlung,
5. Kennzeichnung verspäteter oder fehlender Daten.

FastF1 allein ist hierfür nicht die empfohlene Basis.

## Quellen

- OpenF1 API-Dokumentation: <https://openf1.org/docs/>
- OpenF1 Zugriff, Aktualität und Limits: <https://openf1.org/#sponsorship>
- FastF1 Dokumentation: <https://docs.fastf1.dev/>
- FastF1 Quellcode, MIT-Lizenz und Hinweis zum inoffiziellen Status: <https://github.com/theOehrly/Fast-F1>
- FastF1 Eventkalender und unterstützte Saisons: <https://docs.fastf1.dev/events.html>
- FastF1 Live-Timing-Recorder: <https://docs.fastf1.dev/livetiming.html>
- Konkurrenzanalyse, öffentliche Repositories und Produkte: [KONKURRENZANALYSE.md](KONKURRENZANALYSE.md)

Alle Aussagen zur Verfügbarkeit von Belgien 2026 sind ein datierter technischer Test und keine Garantie der Anbieter für zukünftige Rennen.

