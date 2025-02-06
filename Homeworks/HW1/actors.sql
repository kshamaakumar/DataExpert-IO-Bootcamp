CREATE TYPE films AS (
                         film TEXT,
                         votes INTEGER,
                         rating REAL,
                         filmid TEXT
                       );


CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE actors (
     actor_name TEXT,
     actor_id TEXT,
     films films[],
     quality_class quality_class,
     is_active BOOLEAN,
     year INTEGER,
     PRIMARY KEY (actor_id)
 );