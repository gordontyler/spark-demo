CREATE KEYSPACE sparkdemo WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE sparkdemo;
CREATE TABLE hits (
    id timeuuid,
    source text,
    url text,
    status int,
    PRIMARY KEY (source, id)
);
CREATE TABLE aggregated_hits (
    url text,
    count int,
    error_count int,
    PRIMARY KEY (url)
);
