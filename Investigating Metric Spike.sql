CREATE DATABASE user_engagement_case;
USE user_engagement_case;

-- creating tables

CREATE TABLE users (
    user_id INT,
    created_at VARCHAR(100),
    company_id INT,
    language VARCHAR(50),
    activated_at VARCHAR(100),
    state VARCHAR(50)
);


CREATE TABLE events (
    user_id INT,
    occurred_at VARCHAR(100),
    event_type VARCHAR(50),
    event_name VARCHAR(100),
    location VARCHAR(50),
    device VARCHAR(50),
    user_type INT
);


CREATE TABLE email_events (
    user_id INT,
    occurred_at VARCHAR(100),
    action VARCHAR(100),
    user_type INT
);

SHOW VARIABLES LIKE 'secure_file_priv';
SET SQL_SAFE_UPDATES = 0;
SELECT * FROM users;

-- loading data into tables

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

ALTER TABLE users ADD COLUMN temp_created_at DATETIME;

UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');

ALTER TABLE users DROP COLUMN created_at;

ALTER TABLE users CHANGE COLUMN temp_created_at created_at DATETIME;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

ALTER TABLE events ADD COLUMN temp_occurred_at DATETIME;

UPDATE events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER TABLE events DROP COLUMN occurred_at; 

ALTER TABLE events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;


LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

ALTER TABLE email_events ADD COLUMN temp_occurred_at DATETIME;

UPDATE email_events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER TABLE email_events DROP COLUMN occurred_at; 

ALTER TABLE email_events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;

-- reading tables

SELECT * FROM users;
SELECT * FROM events;
SELECT * FROM email_events;

-- Tasks

-- 1. Measure the activeness of users on a weekly basis.
    
SELECT
    DATE_FORMAT(occurred_at - INTERVAL WEEKDAY(occurred_at) DAY, '%Y-%m-%d') AS week_start,
    WEEK(occurred_at, 1) AS week_number,
    COUNT(DISTINCT user_id) AS active_users
FROM
    events
GROUP BY
    week_start, week_number
ORDER BY
    week_start;

    
/* The goal here was to check how many users were actively engaging with the platform every week.
We extracted the year and week number for each event using YEAR() and WEEK() functions.
Then we used COUNT(DISTINCT user_id) to count how many unique users were active during each week.
Finally, we grouped by year and week to get weekly data, and sorted it chronologically. */


-- 2. Analyze the growth of users over time for a product.
    
SELECT
    DATE_FORMAT(created_at - INTERVAL WEEKDAY(created_at) DAY, '%Y-%m-%d') AS week_start,
    WEEK(created_at, 1) AS week_number,
    COUNT(user_id) AS new_users
FROM
    users
GROUP BY
    week_start, week_number
ORDER BY
    week_start;


/* We worked with the created_at column from the users table, which represents each user's sign-up date.
We extracted the year and week number just like before.
Then we used COUNT(user_id) to count how many new users joined in each week.
We grouped and sorted the results by year and week for clean weekly growth tracking. */


-- 3. Analyze the retention of users on a weekly basis after signing up for a product.

SELECT  
   signup_week AS 'Signup Week',
   SUM(CASE WHEN retention_week = 0 THEN 1 ELSE 0 END) AS 'Week 0',
   SUM(CASE WHEN retention_week = 1 THEN 1 ELSE 0 END) AS 'Week 1',
   SUM(CASE WHEN retention_week = 2 THEN 1 ELSE 0 END) AS 'Week 2',
   SUM(CASE WHEN retention_week = 3 THEN 1 ELSE 0 END) AS 'Week 3',
   SUM(CASE WHEN retention_week = 4 THEN 1 ELSE 0 END) AS 'Week 4',
   SUM(CASE WHEN retention_week = 5 THEN 1 ELSE 0 END) AS 'Week 5',
   SUM(CASE WHEN retention_week = 6 THEN 1 ELSE 0 END) AS 'Week 6',
   SUM(CASE WHEN retention_week = 7 THEN 1 ELSE 0 END) AS 'Week 7',
   SUM(CASE WHEN retention_week = 8 THEN 1 ELSE 0 END) AS 'Week 8',
   SUM(CASE WHEN retention_week = 9 THEN 1 ELSE 0 END) AS 'Week 9',
   SUM(CASE WHEN retention_week = 10 THEN 1 ELSE 0 END) AS 'Week 10',
   SUM(CASE WHEN retention_week = 11 THEN 1 ELSE 0 END) AS 'Week 11',
   SUM(CASE WHEN retention_week = 12 THEN 1 ELSE 0 END) AS 'Week 12',
   SUM(CASE WHEN retention_week = 13 THEN 1 ELSE 0 END) AS 'Week 13',
   SUM(CASE WHEN retention_week = 14 THEN 1 ELSE 0 END) AS 'Week 14',
   SUM(CASE WHEN retention_week = 15 THEN 1 ELSE 0 END) AS 'Week 15',
   SUM(CASE WHEN retention_week = 16 THEN 1 ELSE 0 END) AS 'Week 16',
   SUM(CASE WHEN retention_week = 17 THEN 1 ELSE 0 END) AS 'Week 17',
   SUM(CASE WHEN retention_week = 18 THEN 1 ELSE 0 END) AS 'Week 18'
FROM (
   SELECT  
     e1.user_id,
     e1.activity_week,
     cohort.cohort_week AS signup_week,
     e1.activity_week - cohort.cohort_week AS retention_week
   FROM (
     SELECT  
       user_id,  
       MIN(EXTRACT(WEEK FROM occurred_at)) AS cohort_week
     FROM events
     GROUP BY user_id
   ) AS cohort
   JOIN (
     SELECT  
       user_id,  
       EXTRACT(WEEK FROM occurred_at) AS activity_week
     FROM events
     GROUP BY user_id, EXTRACT(WEEK FROM occurred_at)
   ) AS e1
   ON cohort.user_id = e1.user_id
) AS weekly_retention
GROUP BY signup_week
ORDER BY signup_week;

/* The goal was to see how many users came back each week after signing up.
First, I found out when each user first signed up by getting the earliest week they were active. That became their signup week.
Next, I compared each user's later activity weeks to their signup week to find out how many weeks later they returned — this gave 
me the retention week.
Finally, I counted how many users were active in each retention week using CASE WHEN inside SUM, and grouped everything by the 
signup week to build the weekly retention table.


-- 4. Measure the activeness of users on a weekly basis per device. */

SELECT
    WEEK(occurred_at) AS week,
    device,
    COUNT(DISTINCT user_id) AS user_engagement
FROM
    events
GROUP BY
    device,
    WEEK(occurred_at)
ORDER BY
    WEEK(occurred_at);

/* The goal was to see how many unique users were active each week on different devices.
I used the WEEK() function to group events by week, and grouped the results by both week and device.
To measure user activity, I counted the distinct number of users (COUNT(DISTINCT user_id)) for each combination of week and device.
This helped break down user engagement by device type across weeks to spot trends or usage patterns. */


-- 5. Analyze how users are engaging with the email service.

SELECT
    action,
    COUNT(DISTINCT user_id) AS unique_users_count,
    COUNT(*) AS total_actions_count
FROM
    email_events
GROUP BY
    action
ORDER BY
    action;

/* The objective was to calculate the number of unique users and the total number of actions performed for each distinct action in the email_events table.

What was done:

COUNT(DISTINCT user_id) was used to count the number of unique users who performed each action.

COUNT(*) was used to count the total occurrences of each action.

The data was grouped by the action column to get these metrics per action.

Finally, the results were ordered by action for easy analysis. */
