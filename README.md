# f1-strat

A Python data pipeline for Formula 1 race-weekend data, with a dashboard for loading, inspecting, and replaying sessions.

## Project goal

Provide an automated pipeline that makes Formula 1 session data and weather forecasts available together with clear origin and consistent timestamps. This provides a reproducible foundation for race-weekend analyses, historical replay, and comparisons between forecasts and observed weather.
Sources used:
- **OpenF1:** session metadata, laps, results, race events, and observed track weather.
- **FastF1:** independent checks of lap and observed weather data.
- **Open-Meteo:** weather forecasts for the circuit and session times.
- **Wikidata:** circuit identity and geographic coordinates for weather requests.

## Verification

- Race results, standings, wins, podiums, and teammate comparisons.
- Lap pace and tyre-stint analysis, including OpenF1/FastF1 comparisons.
- Forecast versus observed track weather.
- Track geometry and historical replay with a shared decision-time boundary.
- Qualifying baseline comparisons and exploratory pit-loss/strategy evaluations.

These analyses verify the data flow and expose limitations. Broader model validation remains open.

## Setup and start

Install Python 3.14 and download or clone this repository.

### Step 1: Install dependencies

Windows PowerShell:

```powershell
cd C:\path\to\f1-strat
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

macOS Terminal:

```bash
cd /path/to/f1-strat
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### Step 2: Start the job service

Keep this terminal running.

Windows PowerShell:

```powershell
cd C:\path\to\f1-strat
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH = "src"
python -m f1_pipeline.job_service
```

macOS Terminal:

```bash
cd /path/to/f1-strat
source .venv/bin/activate
export PYTHONPATH="src"
python -m f1_pipeline.job_service
```

### Step 3: Start the dashboard in a second terminal

Windows PowerShell:

```powershell
cd C:\path\to\f1-strat
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH = "src"
python -m streamlit run src/f1_pipeline/dashboard/app.py
```

macOS Terminal:

```bash
cd /path/to/f1-strat
source .venv/bin/activate
export PYTHONPATH="src"
python -m streamlit run src/f1_pipeline/dashboard/app.py
```

If PowerShell blocks activation, use `.\.venv\Scripts\python.exe` instead of `python` in each command.

### Step 4: Open and load data

Open [localhost:8501](http://localhost:8501). A fresh checkout starts with the bundled Hungaroring demo. Select a weekend or session and use its download action. The complete-weekend button loads all completed sessions plus Sprint/Race replay data.

Optional automatic loading uses a third terminal with the same environment: `python -m f1_pipeline.scheduler --season 2026 --watch --refresh-calendar`. It waits for known event times. Details are in the [scheduling section](docs/schriftliche_abgabe.md#45-jobsteuerung-und-aktualisierung).

### Step 5: profit
