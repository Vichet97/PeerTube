-- PeerTube development database initialization
-- Extensions required by PeerTube
\c peertube_dev
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
