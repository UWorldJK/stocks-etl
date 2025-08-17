APP=stocks-etl

.PHONY: build run once logs shell ps down clean sql

build:
    docker compose build

# One-off run of the ETL
once:
    docker compose run --rm etl

# Start the daily scheduler as a background service
run:
    docker compose up -d scheduler

logs:
    docker compose logs -f scheduler

shell:
    # open a Python shell inside the image
    docker compose run --rm etl python

ps:
    docker compose ps

down:
    docker compose down

clean:
    docker compose down -v

# Quick SQL check (prints last 10 metric rows)
sql:
    docker compose run --rm etl python -c "import duckdb; con=duckdb.connect('data/market.duckdb'); print(con.execute(\\\"SELECT * FROM daily_metrics ORDER BY date DESC, ticker LIMIT 10;\\\").fetchdf())"
