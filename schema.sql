CREATE TABLE IF NOT EXISTS movies (
    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    tomatometer_score INTEGER CHECK(tomatometer_score BETWEEN 0 AND 100),
    audience_score INTEGER CHECK(audience_score BETWEEN 0 AND 100),
    genre TEXT,
    rating TEXT,
    duration TEXT,
    release_date TEXT,
    director TEXT,
    original_language TEXT,
    box_office TEXT,
    distributor TEXT
);

CREATE INDEX IF NOT EXISTS idx_tomatometer ON movies(tomatometer_score);
CREATE INDEX IF NOT EXISTS idx_audience ON movies(audience_score);
CREATE INDEX IF NOT EXISTS idx_genre ON movies(genre);
CREATE INDEX IF NOT EXISTS idx_rating ON movies(rating);