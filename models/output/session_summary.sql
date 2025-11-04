-- models/output/session_summary.sql
{{ config(materialized='table') }}

WITH joined AS (
  SELECT
    u.userId,
    u.sessionId,
    u.channel,
    st.ts,
    ROW_NUMBER() OVER (PARTITION BY u.sessionId ORDER BY st.ts DESC) AS rn
  FROM {{ ref('user_session_channel') }} u
  JOIN {{ ref('session_timestamp') }} st
    ON u.sessionId = st.sessionId
)
SELECT
  userId,
  sessionId,
  channel,
  ts
FROM joined
WHERE rn = 1