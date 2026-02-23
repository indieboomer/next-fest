# Project: Steam Next Fest Intelligence Agent
Objective

Build an automated data collection and monitoring system that tracks all demo games participating in Steam Next Fest during the entire festival week.

The system must collect:
Full list of participating games
Demo app IDs
Metadata (genres, tags, developers, release date, languages, etc.)
AI usage disclosures
Player interest metrics over time
Community signals (followers)
Time-series data for analysis after the event
The goal is to create a structured dataset suitable for:
Competitive analysis
AI vs non-AI usage comparison
Interest ranking
Tag/genre trend analysis
Peak demo concurrency tracking

Functional Requirements
1. Discover Participating Games
Scrape the Steam Next Fest sale page:
https://store.steampowered.com/sale/nextfest
Extract all unique appIDs.
Handle dynamic content (JS-rendered page) using Playwright (Chromium headless).
Scroll until content is fully loaded.
Run once per day to detect newly added games.

2. Enrich Game Metadata
For each discovered appID:
Use:https://store.steampowered.com/api/appdetails?appids={appid}&l=english
Extract most important data for future analysis

3. Save data to a SQlite database

4. Data collected should be ok for partial and future (After next fest) analysis
