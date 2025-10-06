import requests
from sqlalchemy import create_engine, MetaData, Table, Column, Text, BigInteger
from sqlalchemy.dialects.postgresql import insert as pg_insert

# postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>
engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/hn")
meta = MetaData()

problems = Table(
    "problems", meta,
    Column("external_id", Text, primary_key=True),
    Column("title", Text),
    Column("url", Text),
    Column("created_at_i", BigInteger, nullable=False),
    Column("text", Text),
)
meta.create_all(engine)  # creates any tables described in SQLAlchemy MetaData that don't already exist

session = requests.Session()

BASE = "http://hn.algolia.com/api/v1/search_by_date"
params = {
    "tags": "(ask_hn,show_hn)",
    "hitsPerPage": 1000,
    "numericFilters": "created_at_i>1700000000",
    "page": 0
}
response = session.get(BASE, params=params, timeout=(3.05, 10))
response.raise_for_status()
data = response.json()

rows = []
for hit in data["hits"]:
    rows.append({
        "external_id": hit.get("objectID"),
        "title": hit.get("title"),
        "url": hit.get("url"),
        "created_at_i": hit.get("created_at_i"),
        "text": hit.get("story_text")
    })
stmt = pg_insert(problems).values(rows)
stmt = stmt.on_conflict_do_update(
    index_elements=["external_id"],
    set_={
        "title": stmt.excluded.title,
        "url": stmt.excluded.url,
        "created_at_i": stmt.excluded.created_at_i,
        "text": stmt.excluded.text
    },
)
with engine.begin() as conn:  # guarantees atomic commit/rollback and keeps ingest robust
    conn.execute(stmt)
