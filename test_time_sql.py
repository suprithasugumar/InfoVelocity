import requests
import json

def test_sql(sql):
    url = "http://localhost:8888/druid/v2/sql"
    res = requests.post(url, json={"query": sql})
    if res.status_code == 200:
        print("SUCCESS:")
        print(json.dumps(res.json()[:3], indent=2))
        return True
    else:
        print(f"FAILED ({res.status_code}): {res.text}")
        return False

# Test elapsed seconds methods
print("Test 1: TIMESTAMPDIFF(SECOND, prev_time, __time)")
test_sql("""
SELECT
    post_id,
    __time,
    LAG(__time, 1) OVER (PARTITION BY post_id ORDER BY __time) AS prev_time,
    TIMESTAMPDIFF(SECOND, LAG(__time, 1) OVER (PARTITION BY post_id ORDER BY __time), __time) AS elapsed_sec
FROM "hn_snapshots"
LIMIT 5
""")

print("\nTest 2: (TIME_TO_MILLIS(__time) - TIME_TO_MILLIS(LAG(__time, 1) OVER (PARTITION BY post_id ORDER BY __time))) / 1000")
test_sql("""
SELECT
    post_id,
    __time,
    (TIME_TO_MILLIS(__time) - TIME_TO_MILLIS(LAG(__time, 1) OVER (PARTITION BY post_id ORDER BY __time))) / 1000 AS elapsed_sec
FROM "hn_snapshots"
LIMIT 5
""")
