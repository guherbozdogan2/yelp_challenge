CREATE KEYSPACE test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE test.business
(
	address text, 
	attributes_map MAP<text, text>,
	business_id text, 
	categories LIST<text>, 
	city text, 
	hours_map MAP<text, text>,  
	is_open boolean, 
	latitude float, 
	longitude float, 
	name text, 
	postal_code text, 
	review_count int, 
	stars float, 
	state text, 
	PRIMARY KEY(business_id, name)
);


CREATE TABLE test.checkin
(
	business_id text,
	checkin_ts bigint,
     PRIMARY KEY(business_id, checkin_ts)
) ;
 
 

 CREATE TABLE test.photo
(	
	photo_id text,
	business_id text,
	caption text,
	label text,
    PRIMARY KEY(photo_id, business_id,caption)
 ) ;
 
 
 CREATE TABLE test.review
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
 
 CREATE TABLE test.tip
(	
	tip_text text,
	tip_timestamp bigint,
	compliment_count bigint,
	business_id text,
	user_id text,
    PRIMARY KEY(business_id, user_id)
) ;
 
 CREATE TABLE test.user
(
	average_stars double,
	compliment_cool bigint, 
	compliment_cute bigint, 
    compliment_funny bigint, 
    compliment_hot bigint, 
    compliment_list bigint, 
    compliment_more bigint, 
    compliment_note bigint, 
    compliment_photos bigint, 
    compliment_plain bigint, 
    compliment_profile bigint, 
    compliment_writer bigint, 
    cool bigint, 
    elite  text, 
    fans bigint, 
    friends_list List<text>, 
    funny bigint, 
    name text, 
    review_count bigint, 
    useful bigint, 
    user_id text, 
    yelping_since text,
    PRIMARY KEY(user_id, name)
) ;
 
 
 