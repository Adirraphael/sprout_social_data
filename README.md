# Sprout Social Data Pipeline

This project automates the extraction of social media data from the Sprout Social API and loads it into Google BigQuery for reporting and analytics.

## What it does

A Python script (`data.py`) pulls data from all available Sprout Social API endpoints and loads it into BigQuery tables under the `sprout_social` dataset in the `focus-on-energy` project.

The pipeline runs automatically every Monday at 8am UTC via GitHub Actions, replacing all tables with fresh data.

## Data Coverage

Data is available from **January 1, 2026** onwards — this is when APTIM Corp. connected their social profiles to Sprout Social.

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
- Google My Business

## Requesting Additional Date Ranges

Since the Sprout API limits each request to a maximum of 1 year of data, the current pipeline covers January 2026 onwards. If you need data from a specific earlier period, additional API calls can be configured upon request.
