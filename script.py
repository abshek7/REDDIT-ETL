import praw
import sqlite3
import pandas as pd
import json
import logging
from datetime import datetime
import os
from typing import List, Dict, Any
import time
from dotenv import load_dotenv


load_dotenv()



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reddit_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



class RedditETL:
    def __init__(self, client_id: str, client_secret: str, user_agent: str, db_path: str = 'reddit_data.db'):
        """Initialize Reddit ETL pipeline"""
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Create database tables if they don't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            #posts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    subreddit TEXT,
                    score INTEGER,
                    upvote_ratio REAL,
                    num_comments INTEGER,
                    created_utc TIMESTAMP,
                    selftext TEXT,
                    url TEXT,
                    is_video BOOLEAN,
                    is_original_content BOOLEAN,
                    over_18 BOOLEAN,
                    stickied BOOLEAN,
                    locked BOOLEAN,
                    title_length INTEGER,
                    selftext_length INTEGER,
                    has_selftext BOOLEAN,
                    hour_posted INTEGER,
                    day_of_week INTEGER,
                    engagement_rate REAL,
                    score_category TEXT,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            #comments table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    id TEXT PRIMARY KEY,
                    post_id TEXT,
                    author TEXT,
                    body TEXT,
                    score INTEGER,
                    created_utc TIMESTAMP,
                    parent_id TEXT,
                    is_submitter BOOLEAN,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (post_id) REFERENCES posts (id)
                )
            ''')
            
            #
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS subreddit_stats (
                    subreddit TEXT,
                    date DATE,
                    total_posts INTEGER,
                    avg_score REAL,
                    avg_comments REAL,
                    top_post_score INTEGER,
                    PRIMARY KEY (subreddit, date)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            raise
    
    def extract_posts(self, subreddit_name: str, limit: int = 100, sort_type: str = 'hot') -> List[Dict[Any, Any]]:
        """Extract posts from a subreddit"""
        try:
            logger.info(f"Extracting {limit} {sort_type} posts from r/{subreddit_name}")
            
            subreddit = self.reddit.subreddit(subreddit_name)
            posts_data = []
            
            # Get posts based on sort type
            if sort_type == 'hot':
                posts = subreddit.hot(limit=limit)
            elif sort_type == 'new':
                posts = subreddit.new(limit=limit)
            elif sort_type == 'top':
                posts = subreddit.top(limit=limit, time_filter='day')
            else:
                posts = subreddit.hot(limit=limit)
            
            for post in posts:
                post_data = {
                    'id': post.id,
                    'title': post.title,
                    'author': str(post.author) if post.author else '[deleted]',
                    'subreddit': post.subreddit.display_name,
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'created_utc': datetime.fromtimestamp(post.created_utc),
                    'selftext': post.selftext,
                    'url': post.url,
                    'is_video': post.is_video,
                    'is_original_content': post.is_original_content,
                    'over_18': post.over_18,
                    'stickied': post.stickied,
                    'locked': post.locked
                }
                posts_data.append(post_data)
                
                time.sleep(0.1)
            
            logger.info(f"Successfully extracted {len(posts_data)} posts")
            return posts_data
            
        except Exception as e:
            logger.error(f"Error extracting posts: {e}")
            return []
    
    def extract_comments(self, post_id: str, limit: int = 50) -> List[Dict[Any, Any]]:
        """Extract comments from a specific post"""
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)
            
            comments_data = []
            
            for comment in submission.comments.list()[:limit]:
                if hasattr(comment, 'body'): 
                    comment_data = {
                        'id': comment.id,
                        'post_id': post_id,
                        'author': str(comment.author) if comment.author else '[deleted]',
                        'body': comment.body,
                        'score': comment.score,
                        'created_utc': datetime.fromtimestamp(comment.created_utc),
                        'parent_id': comment.parent_id,
                        'is_submitter': comment.is_submitter
                    }
                    comments_data.append(comment_data)
            
            return comments_data
            
        except Exception as e:
            logger.error(f"Error extracting comments for post {post_id}: {e}")
            return []
    
    def transform_data(self, posts_data: List[Dict[Any, Any]]) -> pd.DataFrame:
        """Transform and clean the extracted data"""
        try:
            logger.info("Transforming data...")
            
            df = pd.DataFrame(posts_data)
            
            if df.empty:
                return df
            
            # Data cleaning and transformation
            df['title_length'] = df['title'].str.len()
            df['selftext_length'] = df['selftext'].str.len()
            df['has_selftext'] = df['selftext_length'] > 0
            
            # Extract hour of day from creation time
            df['hour_posted'] = pd.to_datetime(df['created_utc']).dt.hour
            df['day_of_week'] = pd.to_datetime(df['created_utc']).dt.dayofweek
            
            # Calculate engagement rate
            df['engagement_rate'] = df['num_comments'] / (df['score'] + 1)  # +1 to avoid division by zero
        
            df['score_category'] = pd.cut(
                df['score'], 
                bins=[-float('inf'), 0, 10, 100, 1000, float('inf')],
                labels=['Negative', 'Low', 'Medium', 'High', 'Viral']
            )
            
            # Clean text fields
            df['title'] = df['title'].str.replace(r'[^\w\s]', '', regex=True).str.strip()
            df['selftext'] = df['selftext'].fillna('')
            
            logger.info(f"Data transformation completed for {len(df)} posts")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            return pd.DataFrame()
    
    def load_posts(self, df: pd.DataFrame):
        """Load transformed data into database"""
        try:
            logger.info("Loading posts data into database...")
            
            conn = sqlite3.connect(self.db_path)

            df.to_sql('posts', conn, if_exists='append', index=False, method='multi')
            
            conn.close()
            logger.info(f"Successfully loaded {len(df)} posts to database")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
    
    def load_comments(self, comments_data: List[Dict[Any, Any]]):
        """Load comments data into database"""
        try:
            if not comments_data:
                return
                
            logger.info(f"Loading {len(comments_data)} comments into database...")
            
            conn = sqlite3.connect(self.db_path)
            df_comments = pd.DataFrame(comments_data)
            df_comments.to_sql('comments', conn, if_exists='append', index=False)
            conn.close()
            
            logger.info(f"Successfully loaded {len(comments_data)} comments")
            
        except Exception as e:
            logger.error(f"Error loading comments: {e}")
    
    def generate_subreddit_stats(self, subreddit_name: str):
        """Generate and store daily statistics for a subreddit"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = '''
                INSERT OR REPLACE INTO subreddit_stats (subreddit, date, total_posts, avg_score, avg_comments, top_post_score)
                SELECT 
                    subreddit,
                    DATE(created_utc) as date,
                    COUNT(*) as total_posts,
                    AVG(score) as avg_score,
                    AVG(num_comments) as avg_comments,
                    MAX(score) as top_post_score
                FROM posts 
                WHERE subreddit = ? AND DATE(created_utc) = DATE('now')
                GROUP BY subreddit, DATE(created_utc)
            '''
            
            cursor = conn.cursor()
            cursor.execute(query, (subreddit_name,))
            conn.commit()
            conn.close()
            
            logger.info(f"Generated statistics for r/{subreddit_name}")
            
        except Exception as e:
            logger.error(f"Error generating statistics: {e}")
    
    def run_pipeline(self, subreddit_name: str, limit: int = 100, extract_comments: bool = False):
        """Run the complete ETL pipeline"""
        try:
            logger.info(f"Starting ETL pipeline for r/{subreddit_name}")
            
            # E
            posts_data = self.extract_posts(subreddit_name, limit)
            
            if not posts_data:
                logger.warning("No posts extracted, stopping pipeline")
                return
            
            # T
            transformed_df = self.transform_data(posts_data)
            
            if transformed_df.empty:
                logger.warning("No data after transformation, stopping pipeline")
                return
            
            # L
            self.load_posts(transformed_df)
            
            if extract_comments:
                logger.info("Extracting comments for top 10 posts...")
                top_posts = transformed_df.nlargest(10, 'score')['id'].tolist()
                
                for post_id in top_posts:
                    comments = self.extract_comments(post_id, limit=20)
                    self.load_comments(comments)
                    time.sleep(1)  # Rate limiting
            
            # Generate statistics
            self.generate_subreddit_stats(subreddit_name)
            
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
            raise

def main():
    """Main function to run the ETL pipeline"""

    CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
    CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
    USER_AGENT = os.getenv("REDDIT_USER_AGENT")
    
    etl = RedditETL(CLIENT_ID, CLIENT_SECRET, USER_AGENT)
    subreddits = ['python', 'datascience', 'MachineLearning']  # Subreddits to analyze
    posts_per_subreddit = 50
    
    for subreddit in subreddits:
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing r/{subreddit}")
            logger.info(f"{'='*50}")
            
            etl.run_pipeline(
                subreddit_name=subreddit,
                limit=posts_per_subreddit,
                extract_comments=True
            )
            
        except Exception as e:
            logger.error(f"Failed to process r/{subreddit}: {e}")
            continue
    
    logger.info("\nETL pipeline execution completed!")
    logger.info(f"Data stored in: {etl.db_path}")

if __name__ == "__main__":
    main()