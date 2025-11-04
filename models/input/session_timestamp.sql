{{ config(materialized='ephemeral') }}

SELECT
  SESSION_ID AS sessionId,
  EVENT_TS   AS ts
FROM {{ source('raw', 'session_timestamp') }}
WHERE SESSION_ID IS NOT NULL
