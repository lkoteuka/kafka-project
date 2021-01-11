select
    from_unixtime(unix_timestamp()) as curr_time,
    t.university                    as university_name,
    t.subject                       as subject_name,
    mean(score)                     as score_value
from test_topic t
group by
    t.university,
    t.subject