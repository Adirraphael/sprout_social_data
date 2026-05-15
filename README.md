# Sprout Social Data Pipeline

This project automates the extraction of social media data from the Sprout Social API and loads it into Google BigQuery for reporting and analytics.

## What it does

A Python script (`data.py`) pulls data from all available Sprout Social API endpoints and loads it into BigQuery tables under the `sprout_social` dataset in the `focus-on-energy` project.

The pipeline runs automatically every Monday at 8am UTC via GitHub Actions, replacing all tables with fresh data.

## Data Coverage

Data is available from **January 1, 2026** onwards — this is when APTIM Corp. connected their social profiles to Sprout Social.

> **Note:** The Sprout API has a maximum date range of **1 year per request**. The current script pulls data from January 2026 to today. If historical data beyond 1 year is needed in the future, additional API calls can be added to cover earlier date ranges upon request.

## BigQuery Tables

| Table | Description | Refresh Method |
|-------|-------------|----------------|
| `profiles` | All connected social profiles | Replace |
| `tags` | Message tags created in Sprout | Replace |
| `groups` | Profile groups | Replace |
| `users` | Active Sprout users | Replace |
| `teams` | Teams set up in Sprout | Replace |
| `queues` | Case queues | Replace |
| `topics` | Listening topics | Replace |
| `profile_analytics` | Daily profile metrics by network | Replace |
| `post_analytics` | Individual post performance metrics | Replace |
| `messages` | All inbox messages sent and received | Replace |
| `cases` | Customer support cases | Replace |
| `listening_messages` | Messages from listening topics | Replace |
| `listening_metrics` | Aggregated listening metrics by day | Replace |

## Networks Covered

- LinkedIn
- Facebook
- Instagram
- YouTube
- Twitter / X
- Google My Business (last 30 days only — API limitation)

## Networks NOT covered

- Paid / Ad data (not available via Sprout API)
- Google My Business profile analytics (not supported by API)
- Yelp, TripAdvisor, Glassdoor (API restriction)
- X / Twitter listening data (API restriction)

## Automation

The pipeline runs via **GitHub Actions** every Monday at 8am UTC. It can also be triggered manually from the Actions tab in GitHub.

All credentials are stored securely as GitHub Secrets and are never exposed in the code.

## Tech Stack

- Python 3.10
- Google BigQuery
- GitHub Actions
- Sprout Social API

## Requesting Additional Date Ranges

Since the Sprout API limits each request to a maximum of 1 year of data, the current pipeline covers January 2026 onwards. If you need data from a specific earlier period, additional API calls can be configured upon request.