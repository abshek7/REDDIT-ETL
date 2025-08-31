-- SQLite
-- Reddit ETL Pipeline - Analysis SQL Queries
-- Data analysis queries for the Reddit ETL pipeline

-- 1. Basic data overview
SELECT 
    subreddit,
    COUNT(*) as total_posts,
    AVG(score) as avg_score,
    AVG(num_comments) as avg_comments,
    MAX(score) as highest_score,
    MIN(score) as lowest_score
FROM posts 
GROUP BY subreddit
ORDER BY total_posts DESC;

-- 2. Top performing posts by subreddit
SELECT 
    subreddit,
    title,
    author,
    score,
    num_comments,
    upvote_ratio,
    created_utc
FROM posts 
WHERE score IN (
    SELECT MAX(score) 
    FROM posts p2 
    WHERE p2.subreddit = posts.subreddit
)
ORDER BY score DESC;

-- 3. Posting patterns by hour of day
SELECT 
    hour_posted,
    COUNT(*) as post_count,
    AVG(score) as avg_score,
    AVG(num_comments) as avg_comments
FROM posts 
GROUP BY hour_posted 
ORDER BY hour_posted;

-- 4. Posting patterns by day of week (0=Monday, 6=Sunday)
SELECT 
    day_of_week,
    CASE day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday' 
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END as day_name,
    COUNT(*) as post_count,
    AVG(score) as avg_score
FROM posts 
GROUP BY day_of_week 
ORDER BY day_of_week;

-- -- 5. Most active authors
-- SELECT 
--     author,
--     COUNT(*) as post_count,
--     AVG(score) as avg_score,
--     SUM(num_comments) as total_comments_received
-- FROM posts 
-- WHERE author != '[deleted]'
-- GROUP BY author 
-- HAVING post_count > 1
-- ORDER BY post_count ,avg_score DESC,
-- LIMIT 20;

-- 6. Content type analysis
SELECT 
    subreddit,
    SUM(CASE WHEN is_video = 1 THEN 1 ELSE 0 END) as video_posts,
    SUM(CASE WHEN has_selftext = 1 THEN 1 ELSE 0 END) as text_posts,
    SUM(CASE WHEN is_original_content = 1 THEN 1 ELSE 0 END) as oc_posts,
    SUM(CASE WHEN over_18 = 1 THEN 1 ELSE 0 END) as nsfw_posts,
    COUNT(*) as total_posts
FROM posts 
GROUP BY subreddit;

-- 7. Engagement analysis
SELECT 
    subreddit,
    score_category,
    COUNT(*) as post_count,
    AVG(engagement_rate) as avg_engagement_rate,
    AVG(upvote_ratio) as avg_upvote_ratio
FROM posts 
GROUP BY subreddit, score_category 
ORDER BY subreddit, score_category;

-- 8. Comment analysis (if comments were extracted)
SELECT 
    p.subreddit,
    COUNT(c.id) as total_comments,
    AVG(c.score) as avg_comment_score,
    COUNT(DISTINCT c.author) as unique_commenters
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY p.subreddit;

-- 9. Top commenters by subreddit
SELECT 
    p.subreddit,
    c.author,
    COUNT(c.id) as comment_count,
    AVG(c.score) as avg_comment_score
FROM posts p
JOIN comments c ON p.id = c.post_id
WHERE c.author != '[deleted]'
GROUP BY p.subreddit, c.author
HAVING comment_count >= 3
ORDER BY p.subreddit, comment_count DESC;

-- 10. Time-based trends
SELECT 
    DATE(created_utc) as date,
    subreddit,
    COUNT(*) as daily_posts,
    AVG(score) as avg_daily_score,
    MAX(score) as max_daily_score
FROM posts 
GROUP BY DATE(created_utc), subreddit 
ORDER BY date DESC, subreddit;

-- 11. Text analysis - post title insights
SELECT 
    subreddit,
    AVG(title_length) as avg_title_length,
    AVG(CASE WHEN title LIKE '%?%' THEN 1 ELSE 0 END) as question_rate,
    AVG(CASE WHEN UPPER(title) = title THEN 1 ELSE 0 END) as all_caps_rate
FROM posts 
GROUP BY subreddit;

-- 12. Correlation between post characteristics and performance
SELECT 
    subreddit,
    CORR(title_length, score) as title_length_score_corr,
    CORR(selftext_length, score) as selftext_length_score_corr,
    CORR(hour_posted, score) as hour_score_corr
FROM posts 
WHERE score > 0
GROUP BY subreddit;

-- 13. Weekly subreddit performance summary
SELECT 
    subreddit,
    COUNT(*) as posts_this_week,
    AVG(score) as avg_score,
    STDDEV(score) as score_std_dev,
    AVG(num_comments) as avg_comments,
    COUNT(DISTINCT author) as unique_authors
FROM posts 
WHERE created_utc >= DATE('now', '-7 days')
GROUP BY subreddit
ORDER BY avg_score DESC;

-- 14. Content quality indicators
SELECT 
    subreddit,
    AVG(CASE WHEN is_original_content = 1 THEN score ELSE NULL END) as avg_oc_score,
    AVG(CASE WHEN is_original_content = 0 THEN score ELSE NULL END) as avg_non_oc_score,
    SUM(is_original_content) * 100.0 / COUNT(*) as oc_percentage
FROM posts 
GROUP BY subreddit;

-- 15. Create views for common queries
CREATE VIEW IF NOT EXISTS post_performance AS
SELECT 
    p.*,
    CASE 
        WHEN score >= 1000 THEN 'Viral'
        WHEN score >= 100 THEN 'Popular'
        WHEN score >= 10 THEN 'Good'
        ELSE 'Low'
    END as performance_tier,
    RANK() OVER (PARTITION BY subreddit ORDER BY score DESC) as rank_in_subreddit
FROM posts p;

 