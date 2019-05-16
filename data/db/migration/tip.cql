
 CREATE TABLE IF NOT EXISTS test.tip
(	
	tip_id text,
	tip_text text,
	tip_timestamp bigint,
	compliment_count bigint,
	business_id text,
	user_id text,
    PRIMARY KEY(business_id, user_id,tip_timestamp,tip_id)
) ;
 