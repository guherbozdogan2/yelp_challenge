CREATE TABLE IF NOT EXISTS test.business
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