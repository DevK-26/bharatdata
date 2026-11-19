"""Run db/schema.sql against a REAL throwaway Postgres and probe its constraints.

Not part of the pytest suite and not a project dependency: it needs
    pip install pgserver psycopg2-binary
(``pgserver`` bundles Postgres binaries; nothing is installed system-wide).
Usage:  python scripts/verify_schema_pg.py [db/schema.sql]
Exits non-zero if the schema fails to load or any probe behaves unexpectedly.
"""
import sys, shutil, pgserver, tempfile, psycopg2
pgdata = tempfile.mkdtemp(prefix="bdpg")
db = pgserver.get_server(pgdata)
conn = psycopg2.connect(db.get_uri()); conn.autocommit = True
cur = conn.cursor()
schema_path = sys.argv[1] if len(sys.argv) > 1 else "db/schema.sql"
cur.execute(open(schema_path).read())
failures = 0
D = "INSERT INTO datasets (slug,title,description,sector,ministry,source_type,resource_id,resource_id_status,source_url,source_format,refresh_cron,expected_schema,licence,attribution) VALUES (%s,'t','d','s','m',%s,%s,%s,'https://x','json','0 3 * * 1','{}','GODL-India','a')"
def attempt(label, sql, args=None, should_fail=True):
    try:
        cur.execute(sql, args); ok = True; msg = ""
    except psycopg2.Error as e:
        ok = False; msg = str(e).splitlines()[0]
    status = "PASS" if ok != should_fail else "FAIL"
    global failures
    failures += status == "FAIL"
    print(f"{status}: {label} -> {'accepted' if ok else 'rejected: '+msg}")
attempt("valid unverified api dataset", D, ("a-ds","api",None,"TODO_VERIFY"), False)
attempt("verified api dataset", D, ("b-ds","api","00000000-0000-0000-0000-000000000000","VERIFIED"), False)
attempt("uuid but TODO_VERIFY", D, ("c-ds","api","00000000-0000-0000-0000-000000000001","TODO_VERIFY"))
attempt("api, null id, no status", D, ("d-ds","api",None,None))
attempt("bad slug", D, ("Bad Slug","api",None,"TODO_VERIFY"))
attempt("duplicate slug", D, ("a-ds","api",None,"TODO_VERIFY"))
attempt("file source with id", D, ("e-ds","file","00000000-0000-0000-0000-000000000002","VERIFIED"))
attempt("run: failed without error", "INSERT INTO fetch_runs (dataset_id,status,finished_at) VALUES (1,'failed',now())")
attempt("run: running with finished_at", "INSERT INTO fetch_runs (dataset_id,status,finished_at) VALUES (1,'running',now())")
attempt("run: succeeded with error", "INSERT INTO fetch_runs (dataset_id,status,finished_at,error) VALUES (1,'succeeded',now(),'x')")
attempt("run: running ok", "INSERT INTO fetch_runs (dataset_id) VALUES (1)", None, False)
attempt("run: failed w/ error", "INSERT INTO fetch_runs (dataset_id,status,finished_at,error) VALUES (1,'failed',now(),'HTTP 503')", None, False)
attempt("snapshot A,B,A", "INSERT INTO snapshots (dataset_id,content_hash,row_count,parquet_path) VALUES (1,repeat('a',64),1,'p1'),(1,repeat('b',64),2,'p2'),(1,repeat('a',64),1,'p3')", None, False)
attempt("snapshot bad hash", "INSERT INTO snapshots (dataset_id,content_hash,row_count,parquet_path) VALUES (1,'xyz',1,'p')")
attempt("snapshot negative rows", "INSERT INTO snapshots (dataset_id,content_hash,row_count,parquet_path) VALUES (1,repeat('a',64),-1,'p')")
attempt("delete dataset with snapshots (RESTRICT)", "DELETE FROM datasets WHERE id=1")
attempt("key revoked w/o timestamp", "INSERT INTO api_keys (key_hash,key_prefix,email,revoked) VALUES (repeat('c',64),'abc','a@b.in',true)")
attempt("key ok", "INSERT INTO api_keys (key_hash,key_prefix,email) VALUES (repeat('c',64),'abc','a@b.in')", None, False)
attempt("key dup hash", "INSERT INTO api_keys (key_hash,key_prefix,email) VALUES (repeat('c',64),'abc','x@b.in')")
attempt("key zero limit", "INSERT INTO api_keys (key_hash,key_prefix,email,daily_limit) VALUES (repeat('d',64),'abc','x@b.in',0)")
attempt("key revoked ok", "INSERT INTO api_keys (key_hash,key_prefix,email,revoked,revoked_at) VALUES (repeat('e',64),'abc','x@b.in',true,now())", None, False)
attempt("user ok", "INSERT INTO users (email) VALUES ('A@b.in')", None, False)
attempt("user dup differing case", "INSERT INTO users (email) VALUES ('a@B.in')")
attempt("user bad email", "INSERT INTO users (email) VALUES ('nope')")
cur.execute("UPDATE datasets SET title='Daily Mandi Prices', description='Wholesale crop prices' WHERE slug='b-ds'")
cur.execute("SELECT slug FROM datasets WHERE search_tsv @@ plainto_tsquery('english','mandi wholesale')")
fts = cur.fetchall()
print("PASS" if fts == [("b-ds",)] else "FAIL", f": full-text search finds the dataset -> {fts}")
failures += fts != [("b-ds",)]
cur.execute("UPDATE datasets SET title='Renamed' WHERE slug='b-ds' RETURNING updated_at > created_at")
bumped = cur.fetchone()[0]
print("PASS" if bumped else "FAIL", ": updated_at trigger bumps on UPDATE")
failures += not bumped
db.cleanup()
shutil.rmtree(pgdata, ignore_errors=True)  # cleanup() only stops the server
print(f"{failures} unexpected result(s)")
sys.exit(1 if failures else 0)
