create table university_score
(
	university_name varchar,
	subject_name varchar,
	score_value double precision,
	curr_time varchar,
	constraint university_score_pk
		unique (university_name, subject_name, curr_time)
);