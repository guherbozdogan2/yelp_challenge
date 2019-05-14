CREATE TABLE test.checkin
(
	business_id text,
	checkin_ts bigint,
     PRIMARY KEY(business_id, checkin_ts)
    ) ;
 