CREATE TABLE test.user3
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
 
 