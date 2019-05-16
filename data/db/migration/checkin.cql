
CREATE TABLE IF NOT EXISTS test.checkin
(
	checkin_id text,
	business_id text,
	checkin_ts bigint,
     PRIMARY KEY(business_id, checkin_ts,checkin_id)
) ;
 