
CREATE KEYSPACE word WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE word.books(name text PRIMARY KEY, content text);

CREATE KEYSPACE databreach WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE databreach.emails(email text, database text, PRIMARY KEY (email, database));
CREATE TABLE databreach.passwords(password text, database text, PRIMARY KEY (password, database));