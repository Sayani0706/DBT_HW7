{{ config(materialized='ephemeral') }}

SELECT
  USER_ID    AS userId,
  SESSION_ID AS sessionId,
  CHANNEL    AS channel
FROM {{ source('raw', 'user_session_channel') }}
WHERE SESSION_ID IS NOT NULL
