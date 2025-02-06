CREATE TABLE host_activity_reduced(
    host TEXT,
    month DATE,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (host, month)
);