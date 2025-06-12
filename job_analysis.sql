CREATE DATABASE OpsAnalyzer;

USE OpsAnalyzer;

CREATE TABLE job_data (
    ds DATE,
    job_id INT,
    actor_id INT,
    event VARCHAR(20),
    language VARCHAR(50),
    time_spent INT,
    org VARCHAR(5)
);


INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org) VALUES
('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');


SELECT * FROM job_data;


-- 1. Calculate the number of jobs reviewed per hour for each day in November 2020.

SELECT ds, 
       COUNT(job_id) AS no_of_jobs, 
       SUM(time_spent) AS total_seconds, 
       SUM(time_spent) / 3600 AS total_hours,
       ROUND(COUNT(job_id) / NULLIF(GREATEST(SUM(time_spent) / 3600, 1), 0),2) AS jobs_per_hour
FROM job_data
WHERE ds BETWEEN "2020-11-01" AND "2020-11-30" 
GROUP BY ds
ORDER BY ds;

/* The goal was to find out how many jobs were reviewed per hour each day in November 2020.
First, we counted the total jobs reviewed (COUNT(job_id)) and summed up the total time spent (SUM(time_spent)).
We converted time from seconds to hours (SUM(time_spent) / 3600) to make calculations easier.
To get jobs reviewed per hour, we divided the total jobs by the total hours.
However, since we had a small dataset, some days had very little time recorded, making the numbers look too high.
To fix this, we made sure the divisor was at least 1 using GREATEST(SUM(time_spent) / 3600, 1), so that small numbers wouldn’t create misleading results.
NULLIF(..., 0) was used to prevent division errors in case there was no time recorded. */


-- 2. Calculate the 7-day rolling average of throughput (number of events per second).

SELECT 
    ds,
    COUNT(job_id) AS total_events,
    SUM(time_spent) AS total_seconds,
    ROUND(COUNT(job_id) / NULLIF(SUM(time_spent), 0), 6) AS throughput_per_second,
    ROUND(
        AVG(COUNT(job_id)) OVER (
            ORDER BY ds
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) / 
        NULLIF(AVG(SUM(time_spent)) OVER (
            ORDER BY ds
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0), 6
    ) AS rolling_avg_throughput_per_second
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
ORDER BY ds;

/*
The goal was to calculate the 7-day rolling average of throughput (events per second).
First, we counted the number of jobs (COUNT(job_id)) and total time spent (SUM(time_spent)) each day.
Then, we calculated the throughput per day by dividing total jobs by total time spent (handling division by zero with NULLIF).
To smooth the results, we used window functions to find the 7-day average of jobs and time spent.
Finally, we divided the 7-day average jobs by 7-day average time to get the rolling average throughput, rounded to 6 decimals. */

-- 3. Calculate the percentage share of each language in the last 30 days.

SELECT 
    language,
    COUNT(*) AS total_jobs,
    ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (), 2) AS percentage_share
FROM job_data
WHERE ds >= DATE_SUB((SELECT MAX(ds) FROM job_data), INTERVAL 30 DAY)
GROUP BY language
ORDER BY percentage_share DESC;

/*
The goal was to calculate the percentage share of each language in the last 30 days.
First, we filtered the data to include only the last 30 days using ds >= DATE_SUB((SELECT MAX(ds), INTERVAL 30 DAY)).
Then, we counted the total number of jobs for each language using COUNT(*).
To find the percentage share, we divided the job count for each language by the total number of jobs (using SUM(COUNT(*)) OVER ()) and multiplied it by 100.
Finally, we rounded the result to 2 decimal places and sorted the output in descending order of percentage share. */


-- 4. Identify duplicate rows in the data

SELECT 
    ds, job_id, actor_id, event, language, time_spent, org,
    COUNT(*) AS duplicate_count
FROM job_data
GROUP BY ds, job_id, actor_id, event, language, time_spent, org
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

/*
The goal was to detect duplicate rows in the job_data table.
Since no column was explicitly marked as a primary key, we had to check for duplicates across all columns.
We grouped the data by every column (ds, job_id, actor_id, event, language, time_spent, org) to find fully identical rows.
Then, we used COUNT(*) to see how many times each exact row appeared.
Using HAVING COUNT(*) > 1, we filtered to show only the rows that had duplicates.
Finally, we ordered the results based on how many times each duplicate occurred. */



