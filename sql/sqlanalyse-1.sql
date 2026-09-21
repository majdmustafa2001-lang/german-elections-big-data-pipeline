-- =========================================
-- 01. Durchschnittlicher Vote Share je Partei/Jahr
-- =========================================

SELECT
    wahljahr,
    partei,
    ROUND(AVG(vote_share), 2) AS avg_vote_share
FROM nrw_scatter
GROUP BY
    wahljahr,
    partei
ORDER BY
    wahljahr,
    avg_vote_share DESC;
    
-- =========================================
-- 02. Veränderung 2017 -> 2022 mit LAG
-- =========================================
    
    
    WITH party_year_avg AS (
    SELECT
        partei,
        wahljahr,
        AVG(vote_share) AS avg_vote_share
    FROM nrw_scatter
    GROUP BY
        partei,
        wahljahr
),

with_previous AS (
    SELECT
        partei,
        wahljahr,
        avg_vote_share,
        LAG(avg_vote_share) OVER (
            PARTITION BY partei
            ORDER BY wahljahr
        ) AS previous_vote_share
    FROM party_year_avg
)

SELECT
    partei,
    wahljahr,
    ROUND(avg_vote_share, 2) AS avg_vote_share,
    ROUND(previous_vote_share, 2) AS previous_vote_share,
    ROUND(avg_vote_share - previous_vote_share, 2) AS change_pp
FROM with_previous
WHERE wahljahr = 2022
ORDER BY change_pp DESC;

-- =========================================
-- 03. Rangveränderung mit RANK()
-- =========================================

WITH party_avg AS (
    SELECT
        wahljahr,
        partei,
        AVG(vote_share) AS avg_vote_share
    FROM nrw_scatter
    GROUP BY
        wahljahr,
        partei
),

ranked AS (
    SELECT
        wahljahr,
        partei,
        avg_vote_share,
        RANK() OVER (
            PARTITION BY wahljahr
            ORDER BY avg_vote_share DESC
        ) AS party_rank
    FROM party_avg
),

r2017 AS (
    SELECT
        partei,
        avg_vote_share AS vote_share_2017,
        party_rank AS rank_2017
    FROM ranked
    WHERE wahljahr = 2017
),

r2022 AS (
    SELECT
        partei,
        avg_vote_share AS vote_share_2022,
        party_rank AS rank_2022
    FROM ranked
    WHERE wahljahr = 2022
)

SELECT
    r2022.partei,
    ROUND(r2017.vote_share_2017, 2) AS vote_share_2017,
    ROUND(r2022.vote_share_2022, 2) AS vote_share_2022,
    r2017.rank_2017,
    r2022.rank_2022,
    CAST(r2017.rank_2017 AS SIGNED)
      - CAST(r2022.rank_2022 AS SIGNED) AS rank_change
FROM r2022
LEFT JOIN r2017
    ON r2022.partei = r2017.partei
ORDER BY rank_2022;

-- =========================================
-- 04.Top 3 Wahlkreise je Partei mit ROW_NUMBER()
-- =========================================

WITH ranked_wahlkreise AS (
    SELECT
        wahljahr,
        partei,
        wahlkreis_id,
        wahlkreis_name,
        vote_share,

        ROW_NUMBER() OVER (
            PARTITION BY wahljahr, partei
            ORDER BY vote_share DESC
        ) AS rn

    FROM nrw_scatter
)

SELECT
    wahljahr,
    partei,
    wahlkreis_id,
    wahlkreis_name,
    ROUND(vote_share, 2) AS vote_share,
    rn AS rang
FROM ranked_wahlkreise
WHERE rn <= 3
ORDER BY
    wahljahr,
    partei,
    rn;
    
-- =========================================
-- 06. Wahlkreis-Veränderungen mit CASE WHEN
-- =========================================

WITH w2017 AS (
    SELECT
        partei,
        wahlkreis_id,
        wahlkreis_name,
        vote_share AS vote_share_2017
    FROM nrw_scatter
    WHERE wahljahr = 2017
),

w2022 AS (
    SELECT
        partei,
        wahlkreis_id,
        wahlkreis_name,
        vote_share AS vote_share_2022
    FROM nrw_scatter
    WHERE wahljahr = 2022
),

changes AS (
    SELECT
        w2022.partei,
        w2022.wahlkreis_id,
        w2022.wahlkreis_name,
        w2017.vote_share_2017,
        w2022.vote_share_2022,
        w2022.vote_share_2022 - w2017.vote_share_2017 AS change_pp

    FROM w2022

    INNER JOIN w2017
        ON w2022.partei = w2017.partei
        AND w2022.wahlkreis_id = w2017.wahlkreis_id
)

SELECT
    partei,
    wahlkreis_id,
    wahlkreis_name,
    ROUND(vote_share_2017, 2) AS vote_share_2017,
    ROUND(vote_share_2022, 2) AS vote_share_2022,
    ROUND(change_pp, 2) AS change_pp,

    CASE
        WHEN change_pp >= 5 THEN 'Starker Gewinn'
        WHEN change_pp > 0 THEN 'Leichter Gewinn'
        WHEN change_pp <= -5 THEN 'Starker Verlust'
        WHEN change_pp < 0 THEN 'Leichter Verlust'
        ELSE 'Unverändert'
    END AS change_category

FROM changes

ORDER BY change_pp DESC;
