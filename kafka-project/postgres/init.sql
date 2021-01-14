create table university_score
(
	university_name varchar,
	subject_name varchar,
	score_value double precision,
	curr_time varchar,
	constraint university_score_pk
		unique (university_name, subject_name, curr_time)
);
create table sms_classifier
(
	curr_time varchar,
	sender_id int,
	sms_text varchar,
	sms_class varchar,
	constraint sms_classifier_pk
		unique (curr_time, sender_id)
);