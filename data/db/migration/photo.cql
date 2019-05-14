CREATE TABLE test.photo
(	
	photo_id text,
	business_id text,
	caption text,
	label text,
    PRIMARY KEY(photo_id, business_id,caption)
 ) ;
 