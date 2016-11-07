CREATE KEYSPACE sparktest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE sparktest;
CREATE TABLE hits (
    id timeuuid,
    source text,
    url text,
    status int,
    PRIMARY KEY (source, id)
);
CREATE TABLE aggregated_hits (
    source text,
    url text,
    count int,
    error_count int,
    PRIMARY KEY (source, url)
);