DROP INDEX IF EXISTS idx_reddit_comments_last_updated;
DROP INDEX IF EXISTS idx_reddit_comments_parent_id;
DROP INDEX IF EXISTS idx_reddit_comments_post_id;

DROP INDEX IF EXISTS idx_reddit_posts_last_updated;
DROP INDEX IF EXISTS idx_reddit_posts_created_utc;
DROP INDEX IF EXISTS idx_reddit_posts_subreddit_post_id;

DROP TABLE IF EXISTS reddit_comments;
DROP TABLE IF EXISTS reddit_posts;