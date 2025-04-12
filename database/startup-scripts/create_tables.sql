
CREATE DATABASE IF NOT EXISTS dt_warehouse;

USE dt_warehouse;

CREATE TABLE last_execution (
    last_execution_date DATE
);

-- INSERT INTO last_execution VALUES('2024-03-02 00:00:00');

-- Landing Table for Submissions
CREATE TABLE load_submissions (
	id INT PRIMARY KEY,
    timestamp timestamp,
    id_contest INT,
    id_problem TEXT,
    problem_name TEXT,
    tags TEXT,
    author TEXT,
    programming_language TEXT,
    verdict TEXT,
    time_consumed INT,
    memory_usage INT
);

-- Staging tables
CREATE TABLE contest_stg (
    id int  NOT NULL,
    name text  NOT NULL,
    start timestamp  NOT NULL,
    duration int  NOT NULL,
    end timestamp  NOT NULL,
    type text  NOT NULL,
    CONSTRAINT Contest_pk PRIMARY KEY (id)
);

-- Table: Problem
CREATE TABLE problem_stg (
    id_problem varchar(64) NOT NULL,
    name text  NOT NULL,
    tags text  NOT NULL,
    CONSTRAINT Problem_pk PRIMARY KEY (id_problem)
);

-- Table: Programming_Language
CREATE TABLE programming_language_stg (
    id int  NOT NULL AUTO_INCREMENT,
    name text  NOT NULL,
    CONSTRAINT Programming_Language_pk PRIMARY KEY (id)
);

-- Table: Submissions
CREATE TABLE submissions_stg (
    id int  NOT NULL,
    timestamp timestamp  NOT NULL,
    id_contest int  NOT NULL,
    id_problem varchar(64) NOT NULL,
    id_author int  NOT NULL,
    id_programming_language int  NOT NULL,
    id_verdict int  NOT NULL,
    time_consumed int  NOT NULL,
    memory_usage int  NOT NULL,
    CONSTRAINT Submissions_pk PRIMARY KEY (id)
);

-- Table: User
CREATE TABLE user_stg (
    id int  NOT NULL AUTO_INCREMENT,
    country TEXT NOT NULL,
    rating int  NOT NULL,
    nickname text  NOT NULL,
    title text  NOT NULL,
    registration_date timestamp  NOT NULL,
    CONSTRAINT User_pk PRIMARY KEY (id)
);

-- Table: Verdict
CREATE TABLE verdict_stg (
    id int  NOT NULL AUTO_INCREMENT,
    name TEXT  NOT NULL,
    CONSTRAINT Verdict_pk PRIMARY KEY (id)
);

-- foreign keys
-- Reference: Submissions_Contest (table: Submissions)
-- ALTER TABLE submissions_stg ADD CONSTRAINT Submissions_Contest FOREIGN KEY Submissions_Contest (id_contest)
--     REFERENCES contest_stg (id);

-- -- Reference: Submissions_Problem (table: Submissions)
-- ALTER TABLE submissions_stg ADD CONSTRAINT Submissions_Problem FOREIGN KEY Submissions_Problem (id_problem)
--     REFERENCES problem_stg (problem_id);

-- -- Reference: Submissions_User (table: Submissions)
-- ALTER TABLE submissions_stg ADD CONSTRAINT Submissions_User FOREIGN KEY Submissions_User (id_author)
--     REFERENCES user_stg (id);

-- -- Reference: Submissions_Verdict (table: Submissions)
-- ALTER TABLE submissions_stg ADD CONSTRAINT Submissions_Verdict FOREIGN KEY Submissions_Verdict (id_verdict)
--     REFERENCES verdict_stg (id);

-- -- Reference: Submissions_programming_language (table: Submissions)
-- ALTER TABLE submissions_stg ADD CONSTRAINT Submissions_programming_language FOREIGN KEY Submissions_programming_language (id_programming_language)
--     REFERENCES programming_language_stg (id);


-- Prod tables

CREATE TABLE contest_prod (
    id int  NOT NULL,
    name text  NOT NULL,
    start timestamp  NOT NULL,
    duration int  NOT NULL,
    end timestamp  NOT NULL,
    type text  NOT NULL,
    CONSTRAINT Contest_pk_prod PRIMARY KEY (id)
);

-- Table: Problem
CREATE TABLE problem_prod (
    id_problem varchar(64)  NOT NULL,
    name text  NOT NULL,
    tags text  NOT NULL,
    CONSTRAINT Problem_pk_prod PRIMARY KEY (id_problem)
);

-- Table: Programming_Language
CREATE TABLE programming_language_prod (
    id int  NOT NULL AUTO_INCREMENT,
    name text  NOT NULL,
    CONSTRAINT Programming_Language_pk_prod PRIMARY KEY (id)
);

-- Table: Submissions
CREATE TABLE submissions_prod (
    id int  NOT NULL,
    timestamp timestamp  NOT NULL,
    id_contest int  NOT NULL,
    id_problem varchar(64)  NOT NULL,
    id_author int  NOT NULL,
    id_programming_language int  NOT NULL,
    id_verdict int  NOT NULL,
    time_consumed int  NOT NULL,
    memory_usage int  NOT NULL,
    CONSTRAINT Submissions_pk_prod PRIMARY KEY (id)
);

-- Table: User
CREATE TABLE user_prod (
    id int  NOT NULL AUTO_INCREMENT,
    country TEXT NOT NULL,
    rating int  NOT NULL,
    nickname text  NOT NULL,
    title text  NOT NULL,
    registration_date timestamp  NOT NULL,
    CONSTRAINT User_pk_prod PRIMARY KEY (id)
);

-- Table: Verdict
CREATE TABLE verdict_prod(
    id int  NOT NULL AUTO_INCREMENT,
    name TEXT  NOT NULL,
    CONSTRAINT Verdict_pk_prod PRIMARY KEY (id)
);

-- foreign keys
-- Reference: Submissions_Contest (table: Submissions)
-- ALTER TABLE submissions_prod ADD CONSTRAINT Submissions_Contest_Prod FOREIGN KEY Submissions_Contest_Prod (id_contest)
--     REFERENCES contest_prod (id);

-- -- Reference: Submissions_Problem (table: Submissions)
-- ALTER TABLE submissions_prod ADD CONSTRAINT Submissions_Problem_Prod FOREIGN KEY Submissions_Problem_Prod (id_problem)
--     REFERENCES problem_prod (problem_id);

-- -- Reference: Submissions_User (table: Submissions)
-- ALTER TABLE submissions_prod ADD CONSTRAINT Submissions_User_Prod FOREIGN KEY Submissions_User_Prod (id_author)
--     REFERENCES user_prod (id);

-- -- Reference: Submissions_Verdict (table: Submissions)
-- ALTER TABLE submissions_prod ADD CONSTRAINT Submissions_Verdict_Prod FOREIGN KEY Submissions_Verdict_Prod (id_verdict)
--     REFERENCES verdict_prod (id);

-- -- Reference: Submissions_programming_language (table: Submissions)
-- ALTER TABLE submissions_prod ADD CONSTRAINT Submissions_programming_language_Prod FOREIGN KEY Submissions_programming_language_Prod (id_programming_language)
--     REFERENCES programming_language_prod (id);


-- STORED PROCEDURES

-- procedure for loading the data from load_submissions to submissions_stg
DELIMITER |
CREATE PROCEDURE InsertSubmissions()
BEGIN

    TRUNCATE TABLE submissions_stg;

	INSERT INTO submissions_stg
		SELECT
			s.id AS id,
			timestamp,
			id_contest,
			id_problem,
			u.id AS id_author,
			p.id AS id_programming_lanaguage,
			v.id AS id_vedict,
			time_consumed,
			memory_usage
		FROM load_submissions s
		INNER JOIN user_stg u ON s.author = u.nickname
		INNER JOIN programming_language_stg p ON s.programming_language = p.name
		INNER JOIN verdict_stg v ON s.verdict = v.name;
END
|
DELIMITER ;


-- Procedure for merging staging tables with prod tables
DELIMITER |
CREATE PROCEDURE MergeStgToProd()
BEGIN
	    -- Loading contest tables
	INSERT INTO contest_prod
    SELECT cs.id,
           cs.name,
           cs.start,
           cs.duration,
           cs.end,
           cs.type
    FROM contest_stg cs
    LEFT JOIN contest_prod cp ON cs.id = cp.id
    WHERE cp.id IS NULL;

    -- Loading problem tables
    INSERT INTO problem_prod
    SELECT s.id_problem, s.name, s.tags
    FROM problem_stg s
    LEFT JOIN problem_prod p
    ON p.id_problem = s.id_problem
    WHERE p.id_problem IS NULL;

    -- Loading language tables
    INSERT INTO programming_language_prod
    SELECT 0, stg.name FROM programming_language_stg stg
    LEFT JOIN programming_language_prod prd
    ON stg.name = prd.name
    WHERE prd.name IS NULL;

    INSERT INTO user_prod
    SELECT
        0,
        stg.country,
        stg.rating,
        stg.nickname,
        stg.title,
        stg.registration_date
    FROM user_stg stg
    LEFT JOIN user_prod prd
    ON stg.nickname = prd.nickname
    WHERE prd.nickname IS NULL;

    INSERT INTO verdict_prod
    SELECT 0, stg.name FROM verdict_stg stg
    LEFT JOIN verdict_prod prd
    ON stg.name = prd.name
    WHERE prd.name IS NULL;

    INSERT INTO submissions_prod
    SELECT
        s.id AS id,
        s.timestamp,
        s.id_contest,
        s.id_problem,
        u.id AS id_author,
        p.id AS id_programming_language,
        v.id AS id_verdict,
        s.time_consumed,
        s.memory_usage
    FROM load_submissions s
        INNER JOIN user_prod u ON s.author = u.nickname
        INNER JOIN programming_language_prod p ON s.programming_language = p.name
        INNER JOIN verdict_prod v ON s.verdict = v.name
        LEFT JOIN submissions_prod sp ON s.id = sp.id
    WHERE sp.id IS NULL;

END
|
DELIMITER ;


-- View for joining submissions with dimensions
CREATE VIEW submissions_joined AS 
SELECT 
	s.id,
    timestamp,
    c.name AS contest_name,
    p.name AS problem_name,
    u.nickname AS user_name,
    pl.name AS programming_language,
    v.name AS verdict,
    time_consumed,
    memory_usage
FROM submissions_prod s
JOIN contest_prod c ON s.id_contest = c.id
JOIN problem_prod p ON s.id_problem = p.id_problem
JOIN user_prod u ON s.id_author = u.id
JOIN programming_language_prod pl ON s.id_programming_language = pl.id
JOIN verdict_prod v ON s.id_verdict = v.id;


-- Views for Power BI

-- Ratio for Programming Language
CREATE VIEW prog_ratio AS 
WITH ok_verdict
AS (
	SELECT programming_language, COUNT(*) AS count_ok
    FROM submissions_joined
    WHERE verdict = 'OK'
    GROUP BY programming_language
),
all_verdicts AS (
	SELECT programming_language, COUNT(*) AS count_all
    FROM submissions_joined
    GROUP BY programming_language
)
SELECT a.programming_language, ROUND((o.count_ok / a.count_all) * 100) AS ratio
FROM all_verdicts a
JOIN ok_verdict o ON o.programming_language = a.programming_language
ORDER BY 2 DESC
;


-- Problems with the biggest amount of submission issues
CREATE VIEW most_error_problem AS
	SELECT problem_name, COUNT(*) AS count_negative_verdicts
	FROM submissions_joined
	WHERE verdict <> 'OK' AND verdict <> 'TESTING'
	GROUP BY problem_name
	ORDER BY 2 DESC
	LIMIT 5
;

-- Top 5 slowest programming languages
CREATE VIEW execution_time AS
	SELECT programming_language, ROUND(AVG(time_consumed) / 1000, 2) AS execution_time
	FROM submissions_joined
	GROUP BY programming_language
	ORDER BY 2 DESC
	LIMIT 5;


-- The most common type of error encountered
CREATE VIEW most_common_error AS
	SELECT verdict, COUNT(*) AS error_cnt
	FROM submissions_joined
    WHERE verdict NOT IN ('OK', 'TESTING', 'SKIPPED')
	GROUP BY verdict
	ORDER BY 2 DESC
    LIMIT 5
    ;

-- Most popular contest
CREATE VIEW most_popular_contest AS
	SELECT type as contest_type, COUNT(*) as contest_count
    FROM contest_prod
    GROUP BY 1;

-- Players with best submission rate
CREATE VIEW top_submission_rate AS
	WITH positive_submissions AS (
		SELECT user_name, COUNT(*) as cnt_positive
        FROM submissions_joined 
        WHERE verdict = 'OK'
        GROUP BY user_name
	),
    all_submissions AS (
		SELECT user_name, COUNT(*) as cnt_all
        FROM submissions_joined 
        GROUP BY user_name
    )
    SELECT a.user_name, p.cnt_positive / a.cnt_all AS successfull_subm_ratio, p.cnt_positive, a.cnt_all
    FROM all_submissions a
    JOIN positive_submissions p 
    ON a.user_name = p.user_name
    WHERE a.cnt_all > 50
    ORDER BY 2 DESC;


-- Correlatiion between Player's Rating and his submission rate
CREATE VIEW subm_rate_rating_correlation AS
	WITH positive_submissions AS (
		SELECT user_name, COUNT(*) as cnt_positive
        FROM submissions_joined 
        WHERE verdict = 'OK'
        GROUP BY user_name
	),
    all_submissions AS (
		SELECT user_name, COUNT(*) as cnt_all
        FROM submissions_joined 
        GROUP BY user_name
    ), 
    player_submissions_joined AS (
		SELECT a.user_name AS user_name, p.cnt_positive / a.cnt_all AS successfull_subm_ratio
		FROM all_submissions a
		JOIN positive_submissions p 
		ON a.user_name = p.user_name
		WHERE a.cnt_all > 50
		ORDER BY 2 DESC
    ),
    correlation_table AS (
		SELECT psj.successfull_subm_ratio * 100 AS successfull_subm_ratio, 
			   u.rating AS rating
		FROM player_submissions_joined psj
		JOIN user_prod u
		ON psj.user_name = u.nickname 
    )
    -- SELECT * FROM correlation_table;
     SELECT sum((successfull_subm_ratio - (SELECT AVG(successfull_subm_ratio) FROM correlation_table)) * (rating - (SELECT AVG(rating) FROM correlation_table))) / ((count(successfull_subm_ratio) -1) * (stddev_samp(successfull_subm_ratio) * stddev_samp(rating))) AS correlation from correlation_table;


-- Topic with the biggest amount of errors
CREATE VIEW topic_with_most_errors AS
WITH tags_verdict AS (
    SELECT p.tags AS tags, v.name as verdict
    FROM submissions_prod s
    JOIN problem_prod p
    ON s.id_problem = p.id_problem 
    JOIN verdict_prod v
    ON s.id_verdict = v.id
), 
tags_unpacked AS (
    select
        SUBSTRING_INDEX(SUBSTRING_INDEX(tags_verdict.tags, ',', numbers.n), ',', -1) tag,
        tags_verdict.verdict
    from
        (select 1 n union all
        select 2 union all select 3 union all
        select 4 union all select 5) numbers INNER JOIN tags_verdict
        on CHAR_LENGTH(tags_verdict.tags)
            -CHAR_LENGTH(REPLACE(tags_verdict.tags, ',', ''))>=numbers.n-1
    )
    SELECT tag, COUNT(*) as count_negative_verdicts
    FROM tags_unpacked 
    WHERE verdict NOT IN ('OK', 'TESTING', 'SKIPPED')
    GROUP BY tag
    ORDER BY 2 DESC;
