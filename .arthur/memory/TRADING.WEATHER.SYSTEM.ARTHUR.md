---
name: Weather trading system (Weatherboy)
description: Systematic temperature trading across Kalshi (29 US cities) and Polymarket (22 intl cities) — infrastructure, lessons learned, data feeds
type: project
---

Arthur runs hundreds of weather market positions on Kalshi ("weatherboy").

**Lesson learned (Mar 21, 2026):** Lost $353 on Miami KMIA because of stale data. Built sophisticated model consuming data from the wrong pipeline. Traders with 1-minute ASOS access saw 81°F at 2:04 PM while Arthur was staring at an IEM feed stuck at 80°F from 45 minutes earlier.

**Data Feed Hierarchy (fastest to slowest):**
1. ASOS 1-minute (OMO/HF-ASOS) — 2-5 min latency — Synoptic/MADIS
2. NWS 5-minute time series — 5 min — weather.gov (C→F rounding issues)
3. Hourly METAR T-group — ~60 min
4. METAR 6-hour max — ~6 hrs
5. DSM (Daily Summary Message) — ~4 PM local
6. CLI (Climatological Report) — next morning (RESOLUTION SOURCE)

**Critical rule:** IEM max_dayairtemp can lag actual ASOS max by HOURS. CLI uses ASOS 2-minute averaged integer °F max directly from sensor.

**29 US Stations (Kalshi KXHIGH):** KATL, KBKF, KAUS, KBOS, KCLT, KMDW, KORD, KDFW, KDAL, KDEN, KDTW, KHOU, KJAX, KLAS, KLAX, KMIA, KMSP, KBNA, KMSY, KNYC, KLGA, KOKC, KPHL, KPHX, KSAT, KSFO, KSEA, KTPA, KDCA

**Three edges:** pre-market ensemble forecast, intraday real-time ASOS, cross-station arbitrage

**Risk limits:** $100/bucket on boundary calls, $300 daily max loss, never average down without confirmed data edge

**API Keys:**
- Synoptic Data token: 1e8c1b6a2eb24fd9a74140cee0008375
- HF-ASOS: requested, pending enablement
- MADIS OMO: requested, pending
