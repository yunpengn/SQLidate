-- Creates the database.
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Creates the tables.
CREATE TABLE a (
    "aID" integer
);

CREATE TABLE b (
    "bID" integer
);

CREATE TABLE c (
    "cID" integer
);

CREATE TABLE d (
    "dID" integer
);

CREATE TABLE e (
    "eID" integer
);

CREATE TABLE f (
    "fID" integer
);

CREATE TABLE g (
    "gID" integer
);

CREATE TABLE h (
    "hID" integer
);

-- Creates the indices.
CREATE INDEX idx_a ON a ("aID");
CREATE INDEX idx_b ON b ("bID");
CREATE INDEX idx_c ON c ("cID");
CREATE INDEX idx_d ON d ("dID");
CREATE INDEX idx_e ON e ("eID");
CREATE INDEX idx_f ON f ("fID");
CREATE INDEX idx_g ON g ("gID");
CREATE INDEX idx_h ON h ("hID");
