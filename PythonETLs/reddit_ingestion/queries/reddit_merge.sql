MERGE INTO `DEST_DATASET_NAME.DEST_TABLE_NAME` AS target
USING `DEST_DATASET_NAME_staging.DEST_TABLE_NAME_staging` AS source
ON target.id = source.id

WHEN NOT MATCHED THEN
    INSERT (
        id,
        subreddit,
        subreddit_id,
        author,
        author_flair_text,
        link_flair_text,
        created_utc,
        retrieved_on,
        title,
        selftext,
        sentiment_score,
        comments
    )
    VALUES (
    source.id,
    source.subreddit,
    source.subreddit_id,
    source.author,
    source.author_flair_text,
    source.link_flair_text,
    source.created_utc,
    source.retrieved_on,
    source.title,
    source.selftext,
    source.sentiment_score,
    source.comments
    );