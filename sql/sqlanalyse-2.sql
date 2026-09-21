DESCRIBE political_activity_timeseries;

-- =========================================
-- Political Activity: Month-over-Month Change
-- =========================================

WITH activity_changes AS (
    SELECT
        month_start,
        bundestag_activity_index,
        youtube_activity_index,

        LAG(bundestag_activity_index) OVER (
            ORDER BY month_start
        ) AS previous_bundestag_index,

        LAG(youtube_activity_index) OVER (
            ORDER BY month_start
        ) AS previous_youtube_index

    FROM political_activity_timeseries
)

SELECT
    month_start,

    ROUND(bundestag_activity_index, 2) AS bundestag_index,
    ROUND(
        bundestag_activity_index - previous_bundestag_index,
        2
    ) AS bundestag_change,

    ROUND(youtube_activity_index, 2) AS youtube_index,
    ROUND(
        youtube_activity_index - previous_youtube_index,
        2
    ) AS youtube_change

FROM activity_changes
ORDER BY month_start;

-- =========================================
-- Top 5 Monate nach Bundestag-Aktivität
-- =========================================

SELECT
    month_start,
    ROUND(bundestag_activity_index, 2) AS bundestag_activity_index,
    activity_count,
    active_person_count
FROM political_activity_timeseries
ORDER BY bundestag_activity_index DESC
LIMIT 5;

-- =========================================
-- Top 5 Monate nach YouTube-Aktivität
-- =========================================

SELECT
    month_start,
    ROUND(youtube_activity_index, 2) AS youtube_activity_index,
    video_count,
    total_views,
    total_likes,
    total_comments
FROM political_activity_timeseries
ORDER BY youtube_activity_index DESC
LIMIT 5;

-- =========================================
-- 3-Monats Moving Average
-- =========================================

SELECT
    month_start,

    ROUND(bundestag_activity_index, 2) AS bundestag_index,

    ROUND(
        AVG(bundestag_activity_index) OVER (
            ORDER BY month_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS bundestag_3m_avg,

    ROUND(youtube_activity_index, 2) AS youtube_index,

    ROUND(
        AVG(youtube_activity_index) OVER (
            ORDER BY month_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS youtube_3m_avg

FROM political_activity_timeseries
ORDER BY month_start;

-- =========================================
-- 3-view erstellen um es in spark zu lesen
-- =========================================

CREATE VIEW vw_party_change_2017_2022 AS
SELECT
    partei,
    wahlkreis_id,
    vote_share
FROM nrw_scatter
WHERE wahljahr = 2022;