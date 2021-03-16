# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SELECT
  SUM(session) AS sessions,
  SUM(CASE 
		WHEN hit > 1 THEN 1
		ELSE 0
		END) AS bounces,
  ROUND((SUM(CASE
		WHEN hit > 1 THEN 1
		ELSE 0
		END)/SUM(session)) * 100, 2) AS bounce_rate
FROM (
  SELECT
    SUM(CASE
        WHEN serverDatetimeLocal < DATETIME_ADD(
					lag_datetime, INTERVAL 30 minute) THEN 0
				ELSE 1
				END) AS session,
    SUM(CASE
			WHEN serverDatetimeLocal <
				DATETIME_ADD(lag_datetime, INTERVAL 30 minute) 
				AND isInteraction IS TRUE THEN 1
      ELSE 0
			END) AS hit,
    clientId
  FROM (
		SELECT 
			*,
			LAG(serverDatetimeLocal) OVER(PARTITION BY clientId ORDER BY
				serverDatetimeLocal) lag_datetime
		FROM
    	`TABLE_NAME`
		GROUP BY
			clientId)
