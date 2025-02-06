CREATE TABLE hosts_cumulated(
    host TEXT,
    host_activity DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);