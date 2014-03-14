CREATE TABLE IF NOT EXISTS manifest (
    link VARCHAR(255),
    time TIMESTAMP WITH TIME ZONE,
    size INTEGER,
    PRIMARY KEY(link));

CREATE TABLE IF NOT EXISTS trade (
    id BIGSERIAL,
    exchange VARCHAR(50),
    currency CHAR(3),
    time TIMESTAMP WITH TIME ZONE,
    price DECIMAL,
    volume DECIMAL,
    cnt INTEGER,
    PRIMARY KEY(id));

CREATE UNIQUE INDEX ON trade (exchange, currency, time, price, volume);
CREATE INDEX ON trade (currency, exchange, time);
CREATE INDEX ON trade (currency, time);
CREATE INDEX ON trade (exchange, time);
CREATE INDEX ON trade (time, currency, exchange);