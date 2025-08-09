APP=stocks-etl

.PHONY: build run once logs shell ps down clean sql

build:
\tdocker compose build

# One-off run of the ETL
once:
\tdocker compose run --rm etl

# Start the daily scheduler as a background service
run:
\tdocker compose up -d scheduler

logs:
\tdocker compose logs -f scheduler

shell:
\t# open a Python shell inside the image
\tdocker compose run --rm etl python

ps:
\tdocker compose ps

down:
\tdocker compose down

clean:
\tdocker compose down -v

# Quick SQL check (prints last 10 metric rows)
sql:
\tdocker compose run --rm etl python -c "import duckdb; con=duckdb.connect('data/market.duckdb'); print(con.execute(\\\"SELECT * FROM daily_metrics ORDER BY date DESC, ticker LIMIT 10;\\\").fetchdf())"
