
 CREATE TABLE IF NOT EXISTS test.review
(	
	review_id text,
	user_id text,
	business_id text,
 	stars float,
	review_timestamp timestamp,
	review_text text,
	useful bigint,
	funny bigint,
	cool bigint,
	PRIMARY KEY(review_id, business_id,user_id)
    ) ;
 