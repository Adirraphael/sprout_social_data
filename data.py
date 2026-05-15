import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, date, timedelta
import time
import os
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================
load_dotenv()

API_TOKEN = os.getenv("SPROUT_API_TOKEN")
CUSTOMER_ID = os.getenv("SPROUT_CUSTOMER_ID")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("GCP_DATASET")
KEY_PATH = os.getenv("GCP_KEY_PATH")
START_DATE = os.getenv("SPROUT_START_DATE", "2026-01-01")
END_DATE = date.today().strftime("%Y-%m-%d")
GMB_START_DATE = (date.today() - timedelta(days=29)).strftime("%Y-%m-%d")

# ============================================================
# SETUP
# ============================================================
headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

def sanitize_columns(df):
    df.columns = [col.replace(".", "_").replace(" ", "_") for col in df.columns]
    return df

def load_to_bq(df, table_name, write_mode):
    if df.empty:
        print(f"  No data for {table_name}, skipping...")
        return
    df = sanitize_columns(df)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode, autodetect=True)
    df = df.astype(str)
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"  ✅ Loaded {len(df)} rows into {table_name}")

def get(url):
    time.sleep(1)
    r = requests.get(url, headers=headers)
    return r.json()

def post(url, body):
    time.sleep(1)
    r = requests.post(url, headers=headers, json=body)
    return r.json()

BASE = "https://api.sproutsocial.com"

# ============================================================
# 1. PROFILES (Replace)
# ============================================================
print("Pulling profiles...")
profiles_response = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer")
all_profiles = profiles_response.get("data", [])
rows = []
for p in all_profiles:
    rows.append({
        "customer_profile_id": p.get("customer_profile_id"),
        "network_type": p.get("network_type"),
        "name": p.get("name"),
        "native_name": p.get("native_name"),
        "native_id": p.get("native_id"),
        "groups": str(p.get("groups", []))
    })
load_to_bq(pd.DataFrame(rows), "profiles", "WRITE_TRUNCATE")
profile_ids = [str(p["customer_profile_id"]) for p in all_profiles]

# Group profiles by network type
profiles_by_network = {}
for p in all_profiles:
    net = p.get("network_type")
    pid = str(p.get("customer_profile_id"))
    if net not in profiles_by_network:
        profiles_by_network[net] = []
    profiles_by_network[net].append(pid)

# ============================================================
# 2. TAGS (Replace)
# ============================================================
print("Pulling tags...")
data = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer/tags")
rows = []
for t in data.get("data", []):
    rows.append({
        "tag_id": t.get("tag_id"),
        "text": t.get("text"),
        "type": t.get("type"),
        "active": t.get("active"),
        "any_group": t.get("any_group"),
        "groups": str(t.get("groups", []))
    })
load_to_bq(pd.DataFrame(rows), "tags", "WRITE_TRUNCATE")

# ============================================================
# 3. GROUPS (Replace)
# ============================================================
print("Pulling groups...")
data = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer/groups")
rows = []
for g in data.get("data", []):
    rows.append({
        "group_id": g.get("group_id"),
        "name": g.get("name")
    })
load_to_bq(pd.DataFrame(rows), "groups", "WRITE_TRUNCATE")
group_ids = [str(g["group_id"]) for g in data.get("data", [])]

# ============================================================
# 4. USERS (Replace)
# ============================================================
print("Pulling users...")
data = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer/users")
rows = []
for u in data.get("data", []):
    rows.append({
        "user_id": u.get("id"),
        "name": u.get("name"),
        "email": u.get("email")
    })
load_to_bq(pd.DataFrame(rows), "users", "WRITE_TRUNCATE")

# ============================================================
# 5. TEAMS (Replace)
# ============================================================
print("Pulling teams...")
data = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer/teams")
rows = []
for t in data.get("data", []):
    rows.append({
        "team_id": t.get("id"),
        "name": t.get("name"),
        "description": t.get("description")
    })
load_to_bq(pd.DataFrame(rows), "teams", "WRITE_TRUNCATE")

# ============================================================
# 6. QUEUES (Replace)
# ============================================================
print("Pulling queues...")
data = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer/queues")
rows = []
for q in data.get("data", []):
    rows.append({
        "queue_id": q.get("id"),
        "name": q.get("name"),
        "description": q.get("description"),
        "associated_teams": str(q.get("associated_teams", []))
    })
load_to_bq(pd.DataFrame(rows), "queues", "WRITE_TRUNCATE")

# ============================================================
# 7. TOPICS (Replace)
# ============================================================
print("Pulling topics...")
data = get(f"{BASE}/v1/{CUSTOMER_ID}/metadata/customer/topics")
rows = []
topic_ids = []
for t in data.get("data", []):
    topic_ids.append(t.get("id"))
    rows.append({
        "topic_id": t.get("id"),
        "name": t.get("name"),
        "topic_type": t.get("topic_type"),
        "description": t.get("description"),
        "group_id": t.get("group_id"),
        "availability_time": t.get("availability_time")
    })
load_to_bq(pd.DataFrame(rows), "topics", "WRITE_TRUNCATE")

# ============================================================
# 8. PROFILE ANALYTICS PER NETWORK (Append)
# ============================================================
print("Pulling profile analytics...")

NETWORK_PROFILE_METRICS = {
    "twitter": [
        "impressions", "likes", "comments_count", "shares_count",
        "reactions", "post_link_clicks", "post_content_clicks",
        "lifetime_snapshot.followers_count", "net_follower_growth",
        "posts_sent_count", "video_views", "engagements_other"
    ],
    "facebook": [
        "impressions", "impressions_organic", "impressions_paid",
        "impressions_unique", "reactions", "comments_count", "shares_count",
        "post_link_clicks", "post_content_clicks_other", "video_views",
        "lifetime_snapshot.followers_count", "lifetime_snapshot.fans_count",
        "net_follower_growth", "followers_gained", "followers_lost",
        "posts_sent_count"
    ],
    "fb_instagram_account": [
        "impressions", "impressions_unique", "impressions_organic",
        "impressions_paid", "likes", "comments_count", "shares_count",
        "saves", "video_views", "story_replies",
        "lifetime_snapshot.followers_count", "net_follower_growth",
        "followers_gained", "followers_lost", "posts_sent_count"
    ],
    "linkedin_company": [
        "impressions", "impressions_unique", "reactions", "comments_count",
        "shares_count", "post_content_clicks",
        "lifetime_snapshot.followers_count", "net_follower_growth",
        "followers_gained", "followers_lost", "posts_sent_count"
    ],
    "youtube": [
        "lifetime_snapshot.followers_count", "net_follower_growth",
        "followers_gained", "followers_lost", "posts_sent_count"
    ]
}

all_analytics_rows = []
for network_type, pids in profiles_by_network.items():
    metrics = NETWORK_PROFILE_METRICS.get(network_type)
    if not metrics:
        print(f"  Skipping profile analytics for {network_type} — not supported")
        continue
    print(f"  Pulling profile analytics for {network_type}...")
    page = 1
    while True:
        body = {
            "filters": [
                f"customer_profile_id.eq({','.join(pids)})",
                f"reporting_period.in({START_DATE}..{END_DATE})"
            ],
            "metrics": metrics,
            "page": page,
            "limit": 1000
        }
        resp = post(f"{BASE}/v1/{CUSTOMER_ID}/analytics/profiles", body)
        if "error" in resp:
            print(f"    Error for {network_type}: {resp['error']}")
            break
        batch = resp.get("data", [])
        if not batch:
            print(f"    No data for {network_type}")
            break
        for item in batch:
            row = {
                "date": item.get("dimensions", {}).get("reporting_period.by(day)"),
                "customer_profile_id": item.get("dimensions", {}).get("customer_profile_id"),
                "network_type": network_type
            }
            for k, v in item.get("metrics", {}).items():
                row[k.replace(".", "_")] = v
            all_analytics_rows.append(row)
        paging = resp.get("paging", {})
        if paging.get("current_page", 1) >= paging.get("total_pages", 1):
            break
        page += 1

load_to_bq(pd.DataFrame(all_analytics_rows), "profile_analytics", "WRITE_APPEND")

# ============================================================
# 9. POST ANALYTICS PER NETWORK (Replace)
# ============================================================
print("Pulling post analytics...")

NETWORK_POST_METRICS = {
    "twitter": [
        "lifetime.impressions", "lifetime.likes", "lifetime.comments_count",
        "lifetime.shares_count", "lifetime.reactions",
        "lifetime.post_link_clicks", "lifetime.post_content_clicks",
        "lifetime.engagements_other", "lifetime.video_views"
    ],
    "facebook": [
        "lifetime.impressions", "lifetime.impressions_organic",
        "lifetime.impressions_paid", "lifetime.impressions_unique",
        "lifetime.reactions", "lifetime.likes", "lifetime.comments_count",
        "lifetime.shares_count", "lifetime.post_link_clicks",
        "lifetime.post_content_clicks", "lifetime.video_views",
        "lifetime.reactions_love", "lifetime.reactions_haha",
        "lifetime.reactions_wow", "lifetime.reactions_sad",
        "lifetime.reactions_angry"
    ],
    "fb_instagram_account": [
        "lifetime.impressions", "lifetime.impressions_unique",
        "lifetime.likes", "lifetime.comments_count", "lifetime.shares_count",
        "lifetime.saves", "lifetime.video_views", "lifetime.reactions",
        "lifetime.reels_unique_session_plays"
    ],
    "linkedin_company": [
        "lifetime.impressions", "lifetime.impressions_unique",
        "lifetime.reactions", "lifetime.comments_count",
        "lifetime.shares_count", "lifetime.post_content_clicks",
        "lifetime.video_views"
    ],
    "youtube": [
        "lifetime.video_views", "lifetime.likes", "lifetime.dislikes",
        "lifetime.comments_count", "lifetime.shares_count",
        "lifetime.subscribers_gained", "lifetime.subscribers_lost",
        "lifetime.estimated_minutes_watched"
    ],
    "google_my_business": [
        "lifetime.impressions", "lifetime.reactions", "lifetime.comments_count"
    ]
}

all_post_rows = []
for network_type, pids in profiles_by_network.items():
    metrics = NETWORK_POST_METRICS.get(network_type)
    if not metrics:
        print(f"  Skipping post analytics for {network_type} — not supported")
        continue
    start = GMB_START_DATE if network_type == "google_my_business" else START_DATE
    print(f"  Pulling post analytics for {network_type}...")
    last_guid = None
    while True:
        body = {
            "filters": [
                f"customer_profile_id.eq({','.join(pids)})",
                f"created_time.in({start}..{END_DATE})"
            ],
            "fields": [
                "created_time", "perma_link", "text", "network",
                "post_type", "post_category", "customer_profile_id",
                "internal.tags.id", "internal.sent_by.email",
                "internal.sent_by.first_name", "internal.sent_by.last_name",
                "guid"
            ],
            "metrics": metrics,
            "sort": ["guid:asc"],
            "limit": 100
        }
        if last_guid:
            body["filters"].append(f"guid.gt({last_guid})")
        resp = post(f"{BASE}/v1/{CUSTOMER_ID}/analytics/posts", body)
        if "error" in resp:
            print(f"    Error for {network_type}: {resp['error']}")
            break
        batch = resp.get("data", [])
        if not batch:
            break
        for item in batch:
            row = {
                "network_type": network_type,
                "guid": item.get("guid"),
                "created_time": item.get("created_time"),
                "text": item.get("text"),
                "network": item.get("network"),
                "post_type": item.get("post_type"),
                "post_category": item.get("post_category"),
                "customer_profile_id": item.get("customer_profile_id"),
                "perma_link": item.get("perma_link"),
                "sent_by_email": item.get("internal", {}).get("sent_by", {}).get("email"),
                "sent_by_name": f"{item.get('internal', {}).get('sent_by', {}).get('first_name', '')} {item.get('internal', {}).get('sent_by', {}).get('last_name', '')}".strip(),
                "tags": str([t.get("id") for t in item.get("internal", {}).get("tags", [])])
            }
            for k, v in item.get("metrics", {}).items():
                row[k.replace(".", "_")] = v
            all_post_rows.append(row)
        last_guid = batch[-1].get("guid")
        if len(batch) < 100:
            break

load_to_bq(pd.DataFrame(all_post_rows), "post_analytics", "WRITE_TRUNCATE")

# ============================================================
# 10. MESSAGES (Append)
# ============================================================
print("Pulling messages...")
rows = []

# Build group to profiles mapping
group_to_profiles = {}
for p in all_profiles:
    pid = str(p.get("customer_profile_id"))
    for g in p.get("groups", []):
        gid = str(g)
        if gid not in group_to_profiles:
            group_to_profiles[gid] = []
        group_to_profiles[gid].append(pid)

for group_id in group_ids:
    group_profile_ids = group_to_profiles.get(group_id, [])
    if not group_profile_ids:
        print(f"  No profiles found for group {group_id}, skipping...")
        continue

    # Split GMB and non-GMB profiles
    gmb_pids = [str(p.get("customer_profile_id")) for p in all_profiles
                if str(p.get("customer_profile_id")) in group_profile_ids
                and p.get("network_type") == "google_my_business"]
    non_gmb_pids = [str(p.get("customer_profile_id")) for p in all_profiles
                    if str(p.get("customer_profile_id")) in group_profile_ids
                    and p.get("network_type") != "google_my_business"]

    # Create list of batches — non-GMB and GMB separately
    profile_batches = []
    if non_gmb_pids:
        profile_batches.append(("non_gmb", non_gmb_pids, START_DATE))
    if gmb_pids:
        profile_batches.append(("gmb", gmb_pids, GMB_START_DATE))

    for batch_type, batch_pids, batch_start in profile_batches:
        print(f"  Pulling messages for group {group_id} ({len(batch_pids)} profiles)...")
        page_cursor = None
        while True:
            body = {
                "filters": [
                    f"group_id.eq({group_id})",
                    f"customer_profile_id.eq({','.join(batch_pids)})",
                    f"created_time.in({batch_start}..{END_DATE})"
                ],
                "fields": [
                    "network", "created_time", "post_category", "post_type",
                    "perma_link", "text", "from", "profile_guid",
                    "customer_profile_id", "guid", "sent",
                    "internal.tags.id", "internal.sent_by.email",
                    "internal.sent_by.first_name", "internal.sent_by.last_name",
                    "language_code", "case_id"
                ],
                "limit": 100,
                "sort": ["created_time:asc"]
            }
            if page_cursor:
                body["page_cursor"] = page_cursor
            resp = post(f"{BASE}/v1/{CUSTOMER_ID}/messages", body)
            if "error" in resp:
                print(f"    Error: {resp['error']}")
                break
            batch = resp.get("data", [])
            if not batch:
                break
            for item in batch:
                rows.append({
                    "group_id": group_id,
                    "guid": item.get("guid"),
                    "network": item.get("network"),
                    "created_time": item.get("created_time"),
                    "post_category": item.get("post_category"),
                    "post_type": item.get("post_type"),
                    "perma_link": item.get("perma_link"),
                    "text": item.get("text"),
                    "customer_profile_id": item.get("customer_profile_id"),
                    "profile_guid": item.get("profile_guid"),
                    "sent": item.get("sent"),
                    "from_name": item.get("from", {}).get("name") if item.get("from") else None,
                    "from_screen_name": item.get("from", {}).get("screen_name") if item.get("from") else None,
                    "sent_by_email": item.get("internal", {}).get("sent_by", {}).get("email"),
                    "sent_by_first_name": item.get("internal", {}).get("sent_by", {}).get("first_name"),
                    "sent_by_last_name": item.get("internal", {}).get("sent_by", {}).get("last_name"),
                    "tags": str([t.get("id") for t in item.get("internal", {}).get("tags", [])]),
                    "language_code": item.get("language_code"),
                    "case_id": item.get("case_id")
                })
            page_cursor = resp.get("paging", {}).get("next_cursor")
            if not page_cursor:
                break

load_to_bq(pd.DataFrame(rows), "messages", "WRITE_APPEND")

# ============================================================
# 11. CASES (Append) - pull week by week
# ============================================================
print("Pulling cases...")
rows = []
start = datetime.strptime(START_DATE, "%Y-%m-%d")
end = datetime.strptime(END_DATE, "%Y-%m-%d")
current = start
while current < end:
    week_end = min(current + timedelta(days=6), end)
    page_cursor = None
    while True:
        body = {
            "filters": [
                f"created_time.in({current.strftime('%Y-%m-%d')}..{week_end.strftime('%Y-%m-%d')})"
            ],
            "sort": ["created_time:asc"],
            "limit": 100
        }
        if page_cursor:
            body["page_cursor"] = page_cursor
        resp = post(f"{BASE}/v1/{CUSTOMER_ID}/cases/filter", body)
        if "error" in resp:
            print(f"    Error: {resp['error']}")
            break
        batch = resp.get("data", [])
        if not batch:
            break
        for item in batch:
            rows.append({
                "case_id": item.get("case_id"),
                "type": item.get("type"),
                "group_id": item.get("group_id"),
                "priority": item.get("priority"),
                "status": item.get("status"),
                "created_by": item.get("created_by"),
                "created_time": item.get("created_time"),
                "updated_time": item.get("updated_time"),
                "last_closed_time": item.get("last_closed_time"),
                "queue_id": item.get("queue_id"),
                "assigned_to": str(item.get("assigned_to", [])),
                "assigned_by": item.get("assigned_by"),
                "tags": str(item.get("tags", []))
            })
        page_cursor = resp.get("paging", {}).get("next_cursor")
        if not page_cursor:
            break
    current += timedelta(days=7)

load_to_bq(pd.DataFrame(rows), "cases", "WRITE_APPEND")

# ============================================================
# 12 & 13. LISTENING (Replace)
# ============================================================
if topic_ids:
    print("Pulling listening data...")

    network_map = {
        "facebook": "FACEBOOK",
        "fb_instagram_account": "INSTAGRAM",
        "linkedin_company": "LINKEDIN",
        "youtube": "YOUTUBE",
        "twitter": "TWITTER"
    }
    all_networks = list(set([
        network_map[n] for n in profiles_by_network.keys()
        if n in network_map
    ]))
    network_filter = ",".join(all_networks)

    all_listening_message_rows = []
    all_listening_metric_rows = []

    for topic_id in topic_ids:
        print(f"  Pulling listening data for topic {topic_id}...")

        # Listening Messages
        page = 1
        while True:
            body = {
                "filters": [
                    f"created_time.in({START_DATE}..{END_DATE})",
                    f"network.eq({network_filter})"
                ],
                "fields": [
                    "created_time", "text", "network", "language",
                    "hashtags", "sentiment", "perma_link",
                    "from.name", "from.screen_name", "from.followers_count",
                    "location.city", "location.country", "content_category"
                ],
                "metrics": [
                    "likes", "replies", "shares_count", "engagements",
                    "positive_sentiments_count", "neutral_sentiments_count",
                    "negative_sentiments_count"
                ],
                "limit": 100,
                "sort": ["created_time:asc"],
                "page": page
            }
            resp = post(f"{BASE}/v1/{CUSTOMER_ID}/listening/topics/{topic_id}/messages", body)
            if "error" in resp:
                print(f"    Error: {resp['error']}")
                break
            batch = resp.get("data", [])
            if not batch:
                break
            for item in batch:
                row = {
                    "topic_id": topic_id,
                    "created_time": item.get("created_time"),
                    "text": item.get("text"),
                    "network": item.get("network"),
                    "perma_link": item.get("perma_link"),
                    "content_category": item.get("content_category"),
                    "language": item.get("listening_metadata", {}).get("language"),
                    "sentiment": item.get("listening_metadata", {}).get("sentiment"),
                    "hashtags": str(item.get("hashtags", [])),
                    "from_name": item.get("from", {}).get("name") if item.get("from") else None,
                    "from_screen_name": item.get("from", {}).get("screen_name") if item.get("from") else None,
                    "from_followers_count": item.get("from", {}).get("followers_count") if item.get("from") else None,
                    "location_city": item.get("location", {}).get("city") if item.get("location") else None,
                    "location_country": item.get("location", {}).get("country") if item.get("location") else None,
                }
                for k, v in item.get("metrics", {}).items():
                    row[k.replace(".", "_")] = v
                all_listening_message_rows.append(row)
            paging = resp.get("paging", {})
            if paging.get("current_page", 1) >= paging.get("total_pages", 1):
                break
            page += 1

        # Listening Metrics
        body = {
            "filters": [
                f"created_time.in({START_DATE}..{END_DATE})",
                f"network.eq({network_filter})"
            ],
            "metrics": [
                "likes", "replies", "shares_count", "engagements",
                "authors_count", "impressions", "messages_count",
                "positive_sentiments_count", "neutral_sentiments_count",
                "negative_sentiments_count"
            ],
            "dimensions": ["created_time.by(day)", "sentiment", "network"]
        }
        resp = post(f"{BASE}/v1/{CUSTOMER_ID}/listening/topics/{topic_id}/metrics", body)
        if "error" not in resp:
            for item in resp.get("data", []):
                row = {
                    "topic_id": topic_id,
                    "date": item.get("dimensions", {}).get("created_time"),
                    "sentiment": item.get("dimensions", {}).get("sentiment"),
                    "network": item.get("dimensions", {}).get("network")
                }
                for k, v in item.get("metrics", {}).items():
                    row[k.replace(".", "_")] = v
                all_listening_metric_rows.append(row)

    load_to_bq(pd.DataFrame(all_listening_message_rows), "listening_messages", "WRITE_TRUNCATE")
    load_to_bq(pd.DataFrame(all_listening_metric_rows), "listening_metrics", "WRITE_TRUNCATE")

else:
    print("No listening topics found, skipping...")

print("\n🎉 All done! Check your sprout_data dataset in BigQuery.")