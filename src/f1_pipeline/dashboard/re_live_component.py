from __future__ import annotations

from collections.abc import Callable
from typing import Any

import streamlit as st

HTML = """
<div class="relive-shell">
  <header class="topbar">
    <button class="home" type="button">F1-Strat</button>
    <div class="event-heading">
      <strong class="event-name"></strong>
      <span class="event-detail"></span>
    </div>
    <section class="weather-strip">
      <div class="weather-current"></div>
      <div class="forecast-grid"></div>
    </section>
  </header>
  <div class="dashboard-grid">
    <aside class="panel left-panel">
      <div class="race-summary"></div>
      <div class="list-title"><span>Position</span><span>Tyres</span><span>Gap</span></div>
      <div class="positions"></div>
    </aside>
    <main class="centre-panel">
      <nav class="view-tabs">
        <button class="tab active" data-view="circle" type="button">Circle of Doom</button>
        <button class="tab" data-view="track" type="button">Track</button>
      </nav>
      <section class="track-panel">
        <svg class="track-svg" viewBox="-1.35 -1.2 2.7 2.4" role="img" aria-label="Race replay">
          <polyline class="track-shadow"></polyline>
          <polyline class="track-line"></polyline>
          <g class="start-line"></g>
          <path class="pit-link"></path>
          <g class="projection-layer"></g>
          <g class="cars-layer"></g>
        </svg>
        <div class="projection-copy"></div>
        <div class="geometry-label"></div>
      </section>
    </main>
    <aside class="panel right-panel">
      <div class="events-title"><span>Race Control</span><span class="event-count"></span></div>
      <div class="events"></div>
    </aside>
  </div>
  <footer class="playback">
    <button class="play" type="button">▶ Play</button>
    <div class="speed-controls">
      <button class="speed active" data-speed="1" type="button">1×</button>
      <button class="speed" data-speed="2" type="button">2×</button>
      <button class="speed" data-speed="5" type="button">5×</button>
      <button class="speed" data-speed="10" type="button">10×</button>
    </div>
    <input class="timeline" type="range" min="0" value="0" step="1" aria-label="Replay time">
    <span class="clock"></span>
  </footer>
  <div class="notification" role="status"></div>
</div>
"""

CSS = """
:host { display: block; color: #f4f7fb; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
* { box-sizing: border-box; }
button { font: inherit; }
.relive-shell { height: 920px; overflow: hidden; border: 1px solid #2b313b; border-radius: 14px; background: #090b0f; color: #f4f7fb; position: relative; }
.topbar { height: 132px; display: grid; grid-template-columns: 118px 270px minmax(0, 1fr); align-items: stretch; border-bottom: 1px solid #2b313b; background: #0e1117; }
.home { margin: 16px; border: 0; background: transparent; color: #e10600; font-size: 24px; font-weight: 850; cursor: pointer; text-align: left; }
.event-heading { display: flex; min-width: 0; flex-direction: column; justify-content: center; gap: 6px; padding: 12px 16px; border-left: 1px solid #242a33; }
.event-name { overflow: hidden; font-size: 18px; text-overflow: ellipsis; white-space: nowrap; }
.event-detail { color: #9aa4b2; font-size: 12px; }
.weather-strip { display: grid; grid-template-columns: 220px minmax(0, 1fr); gap: 8px; padding: 10px; min-width: 0; }
.weather-current, .forecast-tile { border: 1px solid #2a313c; border-radius: 9px; background: #151922; }
.weather-current { padding: 10px 12px; }
.weather-title, .tile-title { color: #9aa4b2; font-size: 10px; font-weight: 750; letter-spacing: .08em; text-transform: uppercase; }
.weather-primary { margin-top: 7px; font-size: 20px; font-weight: 800; }
.weather-secondary, .tile-secondary { margin-top: 4px; color: #c3cad4; font-size: 11px; line-height: 1.4; }
.forecast-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; min-width: 0; }
.forecast-tile { min-width: 0; padding: 9px 10px; }
.tile-primary { margin-top: 7px; font-size: 17px; font-weight: 800; }
.dashboard-grid { height: 712px; display: grid; grid-template-columns: 285px minmax(600px, 1fr) 320px; gap: 10px; padding: 10px; }
.panel, .centre-panel { min-width: 0; overflow: hidden; border: 1px solid #292f39; border-radius: 10px; background: #10141b; }
.left-panel { display: grid; grid-template-rows: auto 26px minmax(0, 1fr); }
.race-summary { padding: 12px; border-bottom: 1px solid #292f39; }
.race-live { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.lap { font-size: 22px; font-weight: 850; }
.status { border-radius: 999px; padding: 4px 9px; background: #1f7a46; color: white; font-size: 10px; font-weight: 800; letter-spacing: .05em; }
.status.neutralized { background: #a27100; }
.status.stopped { background: #a92323; }
.focus-copy { margin-top: 7px; color: #8fd8ff; font-size: 12px; }
.list-title { display: grid; grid-template-columns: 93px minmax(0, 1fr) auto; align-items: center; gap: 5px; padding: 5px 10px; color: #7f8a99; font-size: 10px; font-weight: 750; letter-spacing: .08em; text-transform: uppercase; }
.positions { overflow-y: auto; scrollbar-width: thin; scrollbar-color: #46505e transparent; }
.position-row { min-height: 25px; display: grid; grid-template-columns: 12px 24px 5px 38px minmax(70px, 1fr) auto; align-items: center; gap: 5px; padding: 3px 9px; border-top: 1px solid #1e242d; font-size: 11px; will-change: transform; }
.position-row.focus { background: rgba(78, 184, 235, .14); }
.position-row.inactive { opacity: .48; filter: grayscale(.65); }
.position-change { width: 12px; color: transparent; font-size: 11px; font-weight: 900; line-height: 1; text-align: center; }
.position-change.gained { color: #21a366; animation: position-signal .7s ease both; }
.position-change.lost { color: #e10600; animation: position-signal .7s ease both; }
.position-number { color: #8c96a5; text-align: right; }
.team-mark { width: 5px; height: 17px; border-radius: 4px; }
.driver-code { font-weight: 850; }
.tyre-history { display: flex; min-width: 0; align-items: center; justify-content: flex-start; gap: 4px; }
.tyre-icon { position: relative; width: 15px; height: 15px; flex: 0 0 15px; border: 3px solid var(--tyre-colour); border-radius: 50%; background: #080a0e; cursor: help; box-shadow: inset 0 0 0 1px rgba(255,255,255,.12); }
.tyre-icon::after { content: ''; position: absolute; inset: 3px; border-radius: 50%; background: #202631; }
.tyre-icon.active { box-shadow: 0 0 0 2px #8fd8ff, 0 0 7px rgba(143,216,255,.55), inset 0 0 0 1px rgba(255,255,255,.2); }
.tyre-icon:focus-visible { outline: 2px solid #f4f7fb; outline-offset: 2px; }
.tyre-unavailable { color: #66717f; font-size: 10px; }
.gap { color: #d9dee6; font-variant-numeric: tabular-nums; }
.pit-badge { border-radius: 4px; padding: 2px 4px; background: #735a00; color: #ffe182; font-size: 8px; font-weight: 850; }
.out-badge { color: #a3abb6; font-size: 8px; font-weight: 850; letter-spacing: .08em; }
.centre-panel { display: grid; grid-template-rows: 40px minmax(0, 1fr); }
.view-tabs { display: flex; justify-content: center; gap: 6px; padding: 6px; border-bottom: 1px solid #292f39; }
.tab, .speed, .play { border: 1px solid #343c49; border-radius: 7px; background: #161b24; color: #c9d0da; cursor: pointer; }
.tab { padding: 5px 16px; font-size: 11px; font-weight: 750; }
.tab.active, .speed.active { border-color: #4eb8eb; background: rgba(78, 184, 235, .14); color: #8fd8ff; }
.tab:disabled { cursor: not-allowed; opacity: .38; }
.track-panel { position: relative; min-height: 0; }
.track-svg { width: 100%; height: 100%; overflow: visible; }
.track-shadow { fill: none; stroke: #050608; stroke-linecap: round; stroke-linejoin: round; stroke-width: .12; }
.track-line { fill: none; stroke: #5c6674; stroke-linecap: round; stroke-linejoin: round; stroke-width: .072; }
.pit-link { fill: none; stroke: #8fd8ff; stroke-dasharray: .025 .025; stroke-linecap: round; stroke-width: .018; }
.projection-copy { position: absolute; right: 12px; bottom: 12px; max-width: 280px; padding: 7px 9px; border: 1px solid rgba(143, 216, 255, .35); border-radius: 7px; background: rgba(7, 15, 22, .82); color: #bceaff; font-size: 10px; text-align: right; }
.geometry-label { position: absolute; left: 12px; bottom: 12px; color: #727d8b; font-size: 9px; }
.events-title { height: 42px; display: flex; align-items: center; justify-content: space-between; padding: 0 12px; border-bottom: 1px solid #292f39; font-size: 12px; font-weight: 800; }
.event-count { color: #788391; font-size: 10px; font-weight: 600; }
.events { height: calc(100% - 42px); overflow-y: auto; padding: 5px 9px; scrollbar-width: thin; scrollbar-color: #46505e transparent; }
.event-row { padding: 8px 4px; border-bottom: 1px solid #232a34; }
.event-meta { display: flex; justify-content: space-between; gap: 6px; color: #7f8a99; font-size: 9px; }
.event-message { margin-top: 3px; color: #d7dde6; font-size: 10px; line-height: 1.35; }
.event-flag { color: #ffd35c; font-weight: 800; }
.empty { padding: 16px 8px; color: #737e8d; font-size: 11px; text-align: center; }
.playback { height: 76px; display: grid; grid-template-columns: 88px 190px minmax(0, 1fr) 88px; align-items: center; gap: 12px; padding: 10px 16px; border-top: 1px solid #2b313b; background: #0e1117; }
.play { height: 34px; color: #eef3f8; font-weight: 750; }
.speed-controls { display: flex; gap: 5px; }
.speed { width: 42px; height: 29px; font-size: 10px; font-weight: 750; }
.timeline { width: 100%; accent-color: #e10600; cursor: pointer; }
.clock { font-size: 11px; font-variant-numeric: tabular-nums; text-align: right; }
.notification { position: absolute; left: 50%; bottom: 88px; max-width: 650px; transform: translateX(-50%) translateY(12px); opacity: 0; pointer-events: none; border: 1px solid #e2b53d; border-radius: 8px; background: rgba(24, 20, 8, .96); color: #ffe38a; padding: 10px 18px; font-size: 12px; font-weight: 800; text-align: center; transition: opacity .18s ease, transform .18s ease; }
.notification.visible { transform: translateX(-50%) translateY(0); opacity: 1; }
@keyframes position-signal { 0% { opacity: 0; transform: scale(.65); } 18%, 72% { opacity: 1; transform: scale(1); } 100% { opacity: 0; transform: scale(1.15); } }
@media (prefers-reduced-motion: reduce) { .position-change.gained, .position-change.lost { animation: none; opacity: 1; } }
@media (max-width: 1450px) { .dashboard-grid { grid-template-columns: 250px minmax(520px, 1fr) 285px; } .topbar { grid-template-columns: 105px 230px minmax(0, 1fr); } }
"""

JS = """
const mounted = new WeakMap()

export default function(component) {
  const { data, parentElement, setTriggerValue } = component
  const previous = mounted.get(parentElement)
  if (previous) previous()
  const shell = parentElement.querySelector('.relive-shell')
  if (!shell || !data || !Array.isArray(data.frames) || data.frames.length === 0) return
  const query = (selector) => parentElement.querySelector(selector)
  const queryAll = (selector) => Array.from(parentElement.querySelectorAll(selector))
  const escapeHtml = (value) => String(value ?? '').replace(/[&<>'"]/g, character => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[character]))
  const finite = (value) => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))
  const formatTime = (value, seconds = true) => `${new Date(value).toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:seconds ? '2-digit' : undefined, timeZone:'UTC'})} UTC`
  const frames = data.frames
  const frameTimes = frames.map(frame => Date.parse(frame.date))
  const startTime = frameTimes[0]
  const endTime = frameTimes[frameTimes.length - 1]
  const sessionEnd = Date.parse(data.session?.end || data.race_end)
  const events = Array.isArray(data.race_control) ? data.race_control.map(event => ({...event, time: Date.parse(event.event_time), visibleTime: Math.max(Date.parse(event.event_time), Number(event.available_at_ms))})).filter(event => Number.isFinite(event.time) && Number.isFinite(event.visibleTime)).sort((a, b) => a.visibleTime - b.visibleTime) : []
  const pits = Array.isArray(data.pits) ? data.pits.map(pit => ({...pit, entry: Date.parse(pit.entry_time), exit: Date.parse(pit.exit_time || pit.event_time)})).filter(pit => Number.isFinite(pit.entry) && Number.isFinite(pit.exit) && pit.entry <= pit.exit).sort((a, b) => a.entry - b.entry) : []
  const observations = Array.isArray(data.weather_observations) ? data.weather_observations.map(row => ({...row, time: Date.parse(row.event_time), available: Date.parse(row.available_at || row.event_time)})).filter(row => Number.isFinite(row.time) && Number.isFinite(row.available)).sort((a, b) => a.time - b.time) : []
  const forecasts = Array.isArray(data.forecasts) ? data.forecasts.map(row => ({...row, valid: Date.parse(row.valid_time), available: Date.parse(row.available_at), initialized: Date.parse(row.run_initialized_at), windSpeedMs: finite(row.wind_speed) ? Number(row.wind_speed) / 3.6 : null, rainValue: finite(row.rain) ? Number(row.rain) : finite(row.precipitation) ? Number(row.precipitation) : null})).filter(row => Number.isFinite(row.valid) && Number.isFinite(row.available)).sort((a, b) => a.valid - b.valid) : []
  const tyreStints = new Map()
  for (const stint of Array.isArray(data.tyre_stints) ? data.tyre_stints : []) {
    const driver = Number(stint.driver)
    if (!Number.isFinite(driver) || !finite(stint.start_lap)) continue
    if (!tyreStints.has(driver)) tyreStints.set(driver, [])
    tyreStints.get(driver).push(stint)
  }
  for (const stints of tyreStints.values()) stints.sort((left, right) => Number(left.start_lap) - Number(right.start_lap) || Number(left.stint) - Number(right.stint))
  let currentTime = startTime
  let speed = 1
  let playing = false
  let animationId = null
  let lastWallTime = null
  let view = 'circle'
  let panelFrameIndex = -1
  let lastPanelSecond = -1
  let notificationTimer = null
  let priorRaceTime = startTime
  let previousPositions = new Map()
  let lastPositionRenderTime = null
  const positionAnimations = new Map()
  const positionSignals = new Map()
  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  const timeline = query('.timeline')
  const playButton = query('.play')
  const trackButton = query('[data-view="track"]')
  timeline.max = String(Math.max(1, Math.round((endTime - startTime) / 1000)))
  query('.event-name').textContent = data.meeting?.name || 'Race Re-Live'
  query('.event-detail').textContent = [data.session?.name, data.meeting?.location, data.focus_acronym ? `Focus: ${data.focus_acronym}` : null].filter(Boolean).join(' · ')
  if (!data.geometries?.track) trackButton.disabled = true

  function frameIndexAt(time) {
    let low = 0
    let high = frameTimes.length - 1
    while (low < high) {
      const middle = Math.ceil((low + high) / 2)
      if (frameTimes[middle] <= time) low = middle
      else high = middle - 1
    }
    return low
  }

  function pointAtProgress(points, progress) {
    const wrapped = ((progress % 1) + 1) % 1
    const segments = points.length - 1
    const position = wrapped * segments
    const index = Math.min(Math.floor(position), segments - 1)
    const fraction = position - index
    return [
      points[index][0] + (points[index + 1][0] - points[index][0]) * fraction,
      -(points[index][1] + (points[index + 1][1] - points[index][1]) * fraction),
    ]
  }

  function interpolateProgress(from, to, fraction) {
    let delta = to - from
    if (delta < -0.5) delta += 1
    if (delta > 0.5) delta -= 1
    delta = Math.max(0, delta)
    return ((from + delta * fraction) % 1 + 1) % 1
  }

  function geometry() {
    return data.geometries?.[view] || data.geometries.circle
  }

  function drawGeometry() {
    const selected = geometry()
    const pointText = selected.points.map(point => `${point[0]},${-point[1]}`).join(' ')
    query('.track-shadow').setAttribute('points', pointText)
    query('.track-line').setAttribute('points', pointText)
    const start = pointAtProgress(selected.points, 0)
    query('.start-line').innerHTML = `<line x1="${start[0] - .06}" y1="${start[1]}" x2="${start[0] + .06}" y2="${start[1]}" stroke="#f4f7fb" stroke-width=".018" />`
    query('.geometry-label').textContent = selected.label || ''
  }

  function activePitDrivers(time) {
    const active = new Set()
    for (const pit of pits) {
      if (pit.entry > time) break
      if (time < pit.exit) active.add(Number(pit.driver_number))
    }
    return active
  }

  function tyreHistory(driver, currentLap, inactive = false) {
    const compounds = {
      SOFT: {label: 'Soft', colour: '#e10600'},
      MEDIUM: {label: 'Medium', colour: '#ffd12f'},
      HARD: {label: 'Hard', colour: '#f4f4f4'},
      INTERMEDIATE: {label: 'Intermediate', colour: '#35b759'},
      WET: {label: 'Wet', colour: '#2696ff'},
    }
    const visible = (tyreStints.get(driver) || []).filter(stint => Number(stint.start_lap) <= currentLap)
    if (!visible.length) return '<span class="tyre-unavailable" title="Tyre data unavailable">-</span>'
    return visible.map((stint, index) => {
      const compound = String(stint.compound || 'UNKNOWN').toUpperCase()
      const style = compounds[compound] || {label: 'Unknown compound', colour: '#7f8a99'}
      const startLap = Number(stint.start_lap)
      const endLap = finite(stint.end_lap) ? Number(stint.end_lap) : null
      const active = !inactive && index === visible.length - 1 && (endLap == null || currentLap <= endLap)
      const usedThrough = active ? currentLap : endLap
      const length = usedThrough == null ? null : Math.max(1, usedThrough - startLap + 1)
      const lengthText = length == null ? 'Length unavailable' : `${length} ${length === 1 ? 'lap' : 'laps'}${active ? ' so far' : ''}`
      const rangeText = active ? `Lap ${startLap} - active` : endLap == null ? `From lap ${startLap}` : `Laps ${startLap}-${endLap}`
      const ageText = finite(stint.tyre_age_at_start) ? ` - tyre age at start: ${Number(stint.tyre_age_at_start)} laps` : ''
      const title = `${style.label} - stint ${stint.stint} - ${lengthText} - ${rangeText}${ageText}`
      return `<span class="tyre-icon${active ? ' active' : ''}" style="--tyre-colour:${style.colour}" role="img" tabindex="0" aria-label="${escapeHtml(title)}" title="${escapeHtml(title)}"></span>`
    }).join('')
  }

  function drawCars(index, fraction, time) {
    const from = frames[index]
    const to = frames[Math.min(index + 1, frames.length - 1)]
    const fromCars = new Map(from.cars.map(car => [Number(car[0]), car]))
    const toCars = new Map(to.cars.map(car => [Number(car[0]), car]))
    const selected = geometry()
    const pitDrivers = activePitDrivers(time)
    const fragments = []
    let focusProgress = null
    for (const [driver, car] of fromCars) {
      if (Boolean(car[12])) continue
      const next = toCars.get(driver) || car
      const progress = interpolateProgress(Number(car[5]), Number(next[5]), fraction)
      const point = pointAtProgress(selected.points, progress)
      const focus = driver === Number(data.focus_driver)
      if (focus) focusProgress = progress
      const pitting = pitDrivers.has(driver)
      const colour = /^#?[0-9a-f]{6}$/i.test(String(car[2] || '')) ? `#${String(car[2]).replace('#', '')}` : '#808080'
      fragments.push(`<g transform="translate(${point[0]} ${point[1]})"><circle r="${focus ? '.052' : '.039'}" fill="${escapeHtml(colour)}" stroke="${pitting ? '#ffe182' : focus ? '#8fd8ff' : '#050608'}" stroke-width="${pitting || focus ? '.018' : '.011'}"/><text y="-.055" text-anchor="middle" fill="#f4f7fb" font-size=".052" font-weight="800" paint-order="stroke" stroke="#080a0e" stroke-width=".014">${escapeHtml(car[1])}</text></g>`)
    }
    query('.cars-layer').innerHTML = fragments.join('')
    drawProjection(from, to, selected, focusProgress, fraction)
  }

  function drawProjection(frame, nextFrame, selected, focusProgress, fraction) {
    const copy = query('.projection-copy')
    const layer = query('.projection-layer')
    const link = query('.pit-link')
    if (view !== 'circle' || !frame.projection) {
      layer.innerHTML = ''
      link.setAttribute('d', '')
      const reason = view !== 'circle' ? 'Pit-loss projection is shown in Circle of Doom' : !finite(data.pit_loss_seconds) ? 'Pit-loss projection unavailable for this circuit' : frame.status === 'SC' || frame.status === 'VSC' ? 'Pit-loss projection paused under neutralized race status' : 'Pit-loss projection unavailable without current field gaps'
      copy.textContent = reason
      return
    }
    const projection = frame.projection
    if (!finite(focusProgress) || !finite(frame.reference_lap_time)) return
    const projectedProgress = nextFrame.projection ? interpolateProgress(Number(projection.progress), Number(nextFrame.projection.progress), fraction) : Number(projection.progress)
    const projected = pointAtProgress(selected.points, projectedProgress)
    const progressLoss = Number(projection.loss) / Number(frame.reference_lap_time)
    const arc = []
    for (let index = 0; index <= 28; index += 1) arc.push(pointAtProgress(selected.points, Number(focusProgress) - progressLoss * index / 28))
    link.setAttribute('d', arc.map((point, index) => `${index ? 'L' : 'M'} ${point[0]} ${point[1]}`).join(' '))
    layer.innerHTML = `<g transform="translate(${projected[0]} ${projected[1]})"><circle r=".065" fill="#0b1720" stroke="#8fd8ff" stroke-width=".022"/><text y=".018" text-anchor="middle" fill="#bceaff" font-size=".052" font-weight="850">P${escapeHtml(projection.position)}</text></g>`
    const neighbours = [projection.ahead ? `${projection.gap_ahead.toFixed(1)}s behind ${projection.ahead}` : null, projection.behind ? `${projection.gap_behind.toFixed(1)}s ahead of ${projection.behind}` : null].filter(Boolean).join(' · ')
    copy.textContent = `${data.focus_acronym}: immediate stop +${Number(projection.loss).toFixed(1)}s → P${projection.position}${neighbours ? ` · ${neighbours}` : ''}`
  }

  function renderPositions(frame, time) {
    const pitDrivers = activePitDrivers(time)
    const rows = [...frame.cars].sort((left, right) => Number(left[3]) - Number(right[3]))
    const container = query('.positions')
    const existingRows = new Map(Array.from(container.querySelectorAll('.position-row')).map(row => [Number(row.dataset.driver), row]))
    const oldTops = new Map(Array.from(existingRows, ([driver, row]) => [driver, row.getBoundingClientRect().top]))
    const trackChanges = lastPositionRenderTime != null && time > lastPositionRenderTime && time - lastPositionRenderTime <= 5000
    const animateOrder = trackChanges && !reducedMotion
    const activeDrivers = new Set()
    for (const car of rows) {
      const driver = Number(car[0])
      const position = Number(car[3])
      const inactive = Boolean(car[12])
      const colour = /^#?[0-9a-f]{6}$/i.test(String(car[2] || '')) ? `#${String(car[2]).replace('#', '')}` : '#808080'
      const currentLap = finite(car[4]) ? Number(car[4]) : Number(frame.lap)
      const previousPosition = previousPositions.get(driver)
      const direction = trackChanges && Number.isFinite(previousPosition) && position !== previousPosition ? position < previousPosition ? 'gained' : 'lost' : ''
      if (direction) positionSignals.set(driver, {direction, startedAt: performance.now()})
      let signalState = positionSignals.get(driver)
      const signalAge = signalState ? performance.now() - signalState.startedAt : 0
      if (signalState && signalAge >= 700) {
        positionSignals.delete(driver)
        signalState = null
      }
      const signal = signalState?.direction === 'gained' ? `<span class="position-change gained" style="animation-delay:-${Math.round(signalAge)}ms" role="img" aria-label="Position gained">&#9650;</span>` : signalState?.direction === 'lost' ? `<span class="position-change lost" style="animation-delay:-${Math.round(signalAge)}ms" role="img" aria-label="Position lost">&#9660;</span>` : '<span class="position-change" aria-hidden="true"></span>'
      const row = existingRows.get(driver) || container.ownerDocument.createElement('div')
      row.className = `position-row${driver === Number(data.focus_driver) ? ' focus' : ''}${inactive ? ' inactive' : ''}`
      row.dataset.driver = String(driver)
      row.innerHTML = `${signal}<span class="position-number">${escapeHtml(position)}</span><span class="team-mark" style="background:${escapeHtml(colour)}"></span><span class="driver-code">${escapeHtml(car[1])}</span><span class="tyre-history">${tyreHistory(driver, currentLap, inactive)}</span><span class="gap">${inactive ? '<b class="out-badge">OUT</b>' : pitDrivers.has(driver) ? '<b class="pit-badge">PIT</b>' : escapeHtml(car[7])}</span>`
      container.appendChild(row)
      activeDrivers.add(driver)
      previousPositions.set(driver, position)
    }
    for (const [driver, row] of existingRows) {
      if (activeDrivers.has(driver)) continue
      row.remove()
      previousPositions.delete(driver)
      positionSignals.delete(driver)
    }
    if (animateOrder) {
      for (const row of container.querySelectorAll('.position-row')) {
        const driver = Number(row.dataset.driver)
        const oldTop = oldTops.get(driver)
        if (!Number.isFinite(oldTop)) continue
        const offset = oldTop - row.getBoundingClientRect().top
        if (Math.abs(offset) < 1) continue
        const currentAnimation = positionAnimations.get(driver)
        if (currentAnimation) currentAnimation.cancel()
        const animation = row.animate(
          [{transform: `translateY(${offset}px)`}, {transform: 'translateY(0)'}],
          {duration: 700, easing: 'cubic-bezier(.2,.8,.2,1)'},
        )
        positionAnimations.set(driver, animation)
        const removeAnimation = () => {
          if (positionAnimations.get(driver) === animation) positionAnimations.delete(driver)
        }
        animation.onfinish = removeAnimation
        animation.oncancel = removeAnimation
      }
    } else {
      for (const animation of positionAnimations.values()) animation.cancel()
      positionAnimations.clear()
    }
    lastPositionRenderTime = time
  }

  function renderSummary(frame, time) {
    const status = String(frame.status || 'UNKNOWN')
    const statusClass = ['SC', 'VSC'].includes(status) ? ' neutralized' : ['RED', 'STOPPED'].includes(status) ? ' stopped' : ''
    query('.race-summary').innerHTML = `<div class="race-live"><span class="lap">Lap ${escapeHtml(frame.lap)}</span><span class="status${statusClass}">${escapeHtml(status)}</span></div><div class="focus-copy">Following ${escapeHtml(data.focus_acronym || data.focus_driver)} · ${escapeHtml(formatTime(time))}</div>`
  }

  function latestObservation(time) {
    let selected = null
    for (const row of observations) {
      if (row.time <= time && row.available <= time) selected = row
      if (row.time > time) break
    }
    return selected
  }

  function forecastSnapshot(time) {
    const candidates = forecasts.filter(row => row.available <= time)
    if (!candidates.length) return []
    const latest = candidates.reduce((best, row) => !best || row.available > best.available || row.available === best.available && row.initialized > best.initialized ? row : best, null)
    return candidates.filter(row => row.snapshot_id === latest.snapshot_id)
  }

  function weatherValue(value, digits, suffix) {
    return finite(value) ? `${Number(value).toFixed(digits)}${suffix}` : 'Unavailable'
  }

  // Open-Meteo only publishes hourly points. A forecast tile for a target
  // between two hourly points is linearly interpolated from its two
  // neighbours instead of snapping forward to the next full hour — wind
  // direction is a circular quantity, so it is blended over the shorter arc
  // rather than interpolated as a plain number.
  function circularMeanDegrees(from, to, fraction) {
    const radiansFrom = Number(from) * Math.PI / 180
    const radiansTo = Number(to) * Math.PI / 180
    const x = Math.cos(radiansFrom) * (1 - fraction) + Math.cos(radiansTo) * fraction
    const y = Math.sin(radiansFrom) * (1 - fraction) + Math.sin(radiansTo) * fraction
    return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360
  }

  function interpolateForecast(snapshot, target) {
    let before = null
    let after = null
    for (const row of snapshot) {
      if (row.valid <= target) before = row
      else if (!after) after = row
    }
    if (!before || !after) return null
    if (after.valid === before.valid) {
      return { valid: before.valid, temperature: before.temperature, rain: before.rainValue, wind_speed: before.windSpeedMs, wind_direction: before.wind_direction, interpolated: false }
    }
    const fraction = Math.max(0, Math.min(1, (target - before.valid) / (after.valid - before.valid)))
    const lerp = (a, b) => finite(a) && finite(b) ? Number(a) + (Number(b) - Number(a)) * fraction : null
    return {
      valid: target,
      temperature: lerp(before.temperature, after.temperature),
      rain: lerp(before.rainValue, after.rainValue),
      wind_speed: lerp(before.windSpeedMs, after.windSpeedMs),
      wind_direction: finite(before.wind_direction) && finite(after.wind_direction) ? circularMeanDegrees(before.wind_direction, after.wind_direction, fraction) : null,
      interpolated: fraction > 0.0001 && fraction < 0.9999,
    }
  }

  function renderWeather(time) {
    const current = latestObservation(time)
    const rainfall = current && finite(current.rainfall) ? Number(current.rainfall) ? 'yes' : 'no' : 'Unavailable'
    query('.weather-current').innerHTML = current ? `<div class="weather-title">Current track weather</div><div class="weather-primary">${escapeHtml(weatherValue(current.air_temperature, 1, ' °C'))} air</div><div class="weather-secondary">Track ${escapeHtml(weatherValue(current.track_temperature, 1, ' °C'))} · Rain ${rainfall}<br>Wind ${escapeHtml(weatherValue(current.wind_speed, 1, ' m/s'))} · ${escapeHtml(weatherValue(current.wind_direction, 0, '°'))}</div>` : '<div class="weather-title">Current track weather</div><div class="weather-primary">Unavailable</div><div class="weather-secondary">No observation released at this replay time</div>'
    const snapshot = forecastSnapshot(time)
    const offsets = [[0, 'Now'], [10, '+10 min'], [30, '+30 min'], [60, '+60 min']]
    query('.forecast-grid').innerHTML = offsets.map(([minutes, label]) => {
      const target = time + Number(minutes) * 60000
      const row = target <= sessionEnd ? interpolateForecast(snapshot, target) : null
      if (!row) return `<div class="forecast-tile"><div class="tile-title">${label}</div><div class="tile-primary">Unavailable</div><div class="tile-secondary">No session-relevant forecast</div></div>`
      const note = row.interpolated ? ' · interpolated' : ''
      return `<div class="forecast-tile"><div class="tile-title">${label} · ${escapeHtml(formatTime(row.valid, false))}${note}</div><div class="tile-primary">${escapeHtml(weatherValue(row.temperature, 1, ' °C'))}</div><div class="tile-secondary">Rain ${escapeHtml(weatherValue(row.rain, 1, ' mm'))}<br>Wind ${escapeHtml(weatherValue(row.wind_speed, 1, ' m/s'))} · ${escapeHtml(weatherValue(row.wind_direction, 0, '°'))}</div></div>`
    }).join('')
  }

  function visibleEvents(time) {
    return events.filter(event => event.visibleTime <= time)
  }

  function renderEvents(time) {
    const visible = visibleEvents(time)
    query('.event-count').textContent = String(visible.length)
    query('.events').innerHTML = visible.length ? [...visible].reverse().map(event => `<article class="event-row"><div class="event-meta"><span>${escapeHtml(formatTime(event.time))}${event.lap_number == null ? '' : ` · Lap ${escapeHtml(event.lap_number)}`}</span><span class="event-flag">${escapeHtml(event.flag || event.category || '')}</span></div><div class="event-message">${escapeHtml(event.message || 'Race Control update')}</div></article>`).join('') : '<div class="empty">No Race Control events released yet.</div>'
  }

  function significant(event) {
    const value = `${event.flag || ''} ${event.category || ''} ${event.message || ''}`.toUpperCase()
    return ['YELLOW', 'RED FLAG', 'SAFETY CAR', 'VIRTUAL SAFETY CAR', 'SESSION STARTED', 'SESSION STOPPED'].some(token => value.includes(token))
  }

  function showNotification(event) {
    const notification = query('.notification')
    notification.textContent = [event.flag || event.category, event.message].filter(Boolean).join(' · ')
    notification.classList.add('visible')
    if (notificationTimer) clearTimeout(notificationTimer)
    notificationTimer = setTimeout(() => notification.classList.remove('visible'), 5000)
  }

  function announceBetween(from, to) {
    if (to <= from) return
    const crossed = events.filter(event => event.visibleTime > from && event.visibleTime <= to && significant(event))
    if (crossed.length) showNotification(crossed[crossed.length - 1])
  }

  function renderAt(time, forcePanels = false) {
    currentTime = Math.max(startTime, Math.min(endTime, time))
    const index = frameIndexAt(currentTime)
    const nextIndex = Math.min(index + 1, frames.length - 1)
    const span = Math.max(1, frameTimes[nextIndex] - frameTimes[index])
    const fraction = nextIndex === index ? 0 : Math.max(0, Math.min(1, (currentTime - frameTimes[index]) / span))
    drawCars(index, fraction, currentTime)
    const second = Math.floor(currentTime / 1000)
    if (forcePanels || index !== panelFrameIndex || second !== lastPanelSecond) {
      renderSummary(frames[index], currentTime)
      renderPositions(frames[index], currentTime)
      renderWeather(currentTime)
      renderEvents(currentTime)
      panelFrameIndex = index
      lastPanelSecond = second
    }
    timeline.value = String(Math.round((currentTime - startTime) / 1000))
    query('.clock').textContent = formatTime(currentTime)
  }

  function tick(wallTime) {
    if (!playing) return
    if (lastWallTime == null) lastWallTime = wallTime
    const nextTime = currentTime + (wallTime - lastWallTime) * speed
    lastWallTime = wallTime
    announceBetween(priorRaceTime, Math.min(nextTime, endTime))
    priorRaceTime = Math.min(nextTime, endTime)
    renderAt(nextTime)
    if (currentTime >= endTime) {
      playing = false
      playButton.textContent = '▶ Play'
      animationId = null
      return
    }
    animationId = requestAnimationFrame(tick)
  }

  playButton.onclick = () => {
    playing = !playing
    playButton.textContent = playing ? 'Ⅱ Pause' : '▶ Play'
    lastWallTime = null
    priorRaceTime = currentTime
    if (playing && currentTime >= endTime) renderAt(startTime, true)
    if (playing && animationId == null) animationId = requestAnimationFrame(tick)
    if (!playing && animationId != null) {
      cancelAnimationFrame(animationId)
      animationId = null
    }
  }
  timeline.oninput = event => {
    playing = false
    playButton.textContent = '▶ Play'
    if (animationId != null) cancelAnimationFrame(animationId)
    animationId = null
    renderAt(startTime + Number(event.target.value) * 1000, true)
    priorRaceTime = currentTime
  }
  queryAll('.speed').forEach(button => button.onclick = () => {
    speed = Number(button.dataset.speed)
    queryAll('.speed').forEach(candidate => candidate.classList.toggle('active', candidate === button))
  })
  queryAll('.tab').forEach(button => button.onclick = () => {
    if (button.disabled) return
    view = button.dataset.view
    queryAll('.tab').forEach(candidate => candidate.classList.toggle('active', candidate === button))
    drawGeometry()
    renderAt(currentTime, true)
  })
  query('.home').onclick = () => setTriggerValue('back', Date.now())
  drawGeometry()
  renderAt(startTime, true)
  const cleanup = () => {
    if (animationId != null) cancelAnimationFrame(animationId)
    if (notificationTimer) clearTimeout(notificationTimer)
    for (const animation of positionAnimations.values()) animation.cancel()
    positionAnimations.clear()
    positionSignals.clear()
    mounted.delete(parentElement)
  }
  mounted.set(parentElement, cleanup)
  return cleanup
}
"""

_RE_LIVE = st.components.v2.component(
    "f1_re_live",
    html=HTML,
    css=CSS,
    js=JS,
)


def render_re_live(
    payload: dict[str, Any],
    *,
    key: str,
    on_back: Callable[[], None],
) -> None:
    _RE_LIVE(
        key=key,
        data=payload,
        height=930,
        width="stretch",
        on_back_change=on_back,
    )
