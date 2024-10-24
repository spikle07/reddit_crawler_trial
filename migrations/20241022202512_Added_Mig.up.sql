CREATE TABLE reddit_posts (
    id BIGSERIAL PRIMARY KEY,
    subreddit TEXT NOT NULL,
    post_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    created_utc TIMESTAMPTZ NOT NULL,
    score INTEGER NOT NULL,
    data JSONB NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    crawled_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE reddit_comments (
    id BIGSERIAL PRIMARY KEY,
    post_id TEXT NOT NULL,
    comment_id TEXT NOT NULL UNIQUE,
    parent_id TEXT NOT NULL,
    author TEXT NOT NULL,
    created_utc TIMESTAMPTZ NOT NULL,
    score INTEGER NOT NULL,
    body TEXT NOT NULL,
    data JSONB NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    crawled_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_reddit_posts_subreddit_post_id ON reddit_posts (subreddit, post_id);
CREATE INDEX idx_reddit_posts_created_utc ON reddit_posts (created_utc);
CREATE INDEX idx_reddit_posts_last_updated ON reddit_posts (last_updated);

CREATE INDEX idx_reddit_comments_post_id ON reddit_comments (post_id);
CREATE INDEX idx_reddit_comments_parent_id ON reddit_comments (parent_id);
CREATE INDEX idx_reddit_comments_last_updated ON reddit_comments (last_updated);