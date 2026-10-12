import requests
import json

def execute_query(sql: str):
    url = "http://localhost:8888/druid/v2/sql"
    res = requests.post(url, json={"query": sql})
    if res.status_code == 200:
        return res.json()
    else:
        print(f"ERROR {res.status_code}: {res.text}")
        return None

if __name__ == "__main__":
    query = 'SELECT __time, post_id, author, source, title, url, score, num_comments, v_score, a_score FROM "hn_snapshots" WHERE v_score IS NOT NULL LIMIT 5'
    data = execute_query(query)
    print(json.dumps(data, indent=2))







