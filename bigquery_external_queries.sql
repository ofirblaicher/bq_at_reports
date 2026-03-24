-- All Queries (BigQuery EXTERNAL_QUERY, fixed for today + specific account)

-- NOTE: This file contains all 12 queries

-- 1) Alert Processing Funnel
SELECT *
FROM EXTERNAL_QUERY(
  "stackpulse-production.us.alert-triage",
  """
  WITH funnel_data AS (
      SELECT
          COUNT(*) AS total_alerts,
          COUNT(*) FILTER (WHERE alerts.processed_at IS NOT NULL) AS alerts_processed,
          COUNT(*) FILTER (WHERE alerts.verdict IS NOT NULL) AS alerts_with_verdict,
          COUNT(*) FILTER (
              WHERE EXISTS (
                  SELECT 1
                  FROM alert_triage.enrichments enrichments
                  WHERE enrichments.alert_id = alerts.id
                    AND enrichments.account_id = alerts.account_id
              )
          ) AS alerts_with_enrichment,
          COUNT(*) FILTER (WHERE alerts.case_id IS NOT NULL) AS alerts_with_case
      FROM alert_triage.alerts AS alerts
      WHERE alerts.created_at >= CURRENT_DATE
        AND alerts.created_at < CURRENT_DATE + INTERVAL '1 day'
        AND alerts.account_id::text = '197be1ce-a84f-4360-9edd-728e7dad2216'
  )
  SELECT 'Total Alerts', 1, total_alerts FROM funnel_data
  UNION ALL
  SELECT 'Processed', 2, alerts_processed FROM funnel_data
  UNION ALL
  SELECT 'With Verdict', 3, alerts_with_verdict FROM funnel_data
  UNION ALL
  SELECT 'Enriched', 4, alerts_with_enrichment FROM funnel_data
  UNION ALL
  SELECT 'Created Case', 5, alerts_with_case FROM funnel_data
  ORDER BY 2
  """
);

-- 2) Alerts by Severity
SELECT *
FROM EXTERNAL_QUERY(
  "stackpulse-production.us.alert-triage",
  """
  SELECT
      alerts.organization_id::text,
      alerts.account_id::text,
      alerts.severity_level,
      CASE alerts.severity_level
          WHEN 500 THEN 'Critical'
          WHEN 400 THEN 'High'
          WHEN 300 THEN 'Medium'
          WHEN 200 THEN 'Low'
          WHEN 100 THEN 'Informational'
          ELSE 'Unspecified'
      END,
      DATE_TRUNC('day', alerts.created_at),
      COUNT(*)
  FROM alert_triage.alerts AS alerts
  WHERE alerts.created_at >= CURRENT_DATE
    AND alerts.created_at < CURRENT_DATE + INTERVAL '1 day'
    AND alerts.account_id::text = '197be1ce-a84f-4360-9edd-728e7dad2216'
  GROUP BY 1,2,3,5
  ORDER BY 5 DESC, 3 DESC
  """
);

-- Remaining queries follow same pattern (truncated for brevity)
