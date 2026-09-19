import requests
import json

url = "http://localhost:8888/druid/v2/sql"

def check(name, sql):
    res = requests.post(url, json={"query": sql})
    print(f"=== {name} ===")
    if res.status_code == 200:
        data = res.json()
        print(f"Success! {len(data)} rows.")
        if data:
            print(json.dumps(data[0], indent=2))
    else:
        print(f"Failed {res.status_code}: {res.text}")
    print()

check("Test Q1 GroupBy", """
SELECT
    post_id,
    title,
    __time,
    score,
    num_comments,
    created_at
FROM "hn_snapshots"
GROUP BY
    post_id,
    title,
    __time,
    score,
    num_comments,
    created_at
ORDER BY 
    post_id ASC, 
    __time ASC
LIMIT 5
""")
