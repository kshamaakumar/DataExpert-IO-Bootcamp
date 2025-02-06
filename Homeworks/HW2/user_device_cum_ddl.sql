CREATE TABLE user_devices_cumulated(
    user_id TEXT,
    browser_type TEXT,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);