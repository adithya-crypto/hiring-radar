# backend/app/jobs/run_forecast.py
from sqlalchemy import text
from sqlalchemy.orm import Session

INCLUDE_KEYWORDS = [
    # SWE/SDE keywords: this list matches what you saw in the repo/zip
    "software engineer", "software developer", "swe", "sde",
    "backend", "back-end", "frontend", "front-end", "full stack", "full-stack",
    "platform engineer", "infrastructure engineer", "distributed systems",
    "site reliability engineer", "sre",
    "devops", "developer productivity", "build & release", "build and release",
    "android", "ios", "mobile engineer",
    "data platform engineer", "ml platform engineer",
    "compiler engineer", "systems engineer", "kernel engineer",
]

EXCLUDE_KEYWORDS = [
    # optional filters to make scoring closer to “SDE” jobs
    "intern", "internship", "apprentice",
    "contract", "contractor",
    "qa", "quality assurance", "test engineer",
    "manager", "engineering manager", "director", "vp",
]

def run_forecast_now(db: Session, window_days: int = 28, weight_new: int = 2) -> int:
    """
    Recompute hiring scores into table 'hiring_score' using job_postings.
    Score = open_now + weight_new * new_last_{window_days}.
    Details go into details_json (open_now, new_last_4w, sample_title, evidence_urls).
    Returns number of rows inserted.
    """

    # 1) Prepare keyword arrays for SQL
    kw_incl = "ARRAY[" + ",".join(["lower(%s)" % repr(k) for k in INCLUDE_KEYWORDS]) + "]"
    kw_excl = "ARRAY[" + ",".join(["lower(%s)" % repr(k) for k in EXCLUDE_KEYWORDS]) + "]"

    sql = text(f"""
    WITH base AS (
      SELECT
        jp.company_id,
        jp.title,
        jp.department,
        jp.apply_url,
        coalesce(jp.updated_at, jp.created_at) AS ts,
        jp.closed_at,
        lower(coalesce(jp.title, ''))      AS l_title,
        lower(coalesce(jp.department, '')) AS l_dept
      FROM job_postings jp
    ),
    filtered AS (
      SELECT *
      FROM base b
      WHERE
        -- include if ANY include keyword appears in title or department
        EXISTS (
          SELECT 1
          FROM unnest({kw_incl}) AS kw
          WHERE b.l_title LIKE '%%' || kw || '%%'
             OR b.l_dept  LIKE '%%' || kw || '%%'
        )
        -- and NO exclude keywords appear
        AND NOT EXISTS (
          SELECT 1
          FROM unnest({kw_excl}) AS kw
          WHERE b.l_title LIKE '%%' || kw || '%%'
             OR b.l_dept  LIKE '%%' || kw || '%%'
        )
    ),
    agg AS (
      SELECT
        company_id,
        COUNT(*) FILTER (WHERE closed_at IS NULL) AS open_now,
        COUNT(*) FILTER (WHERE ts > now() - INTERVAL '{window_days} days') AS new_last,
        (array_agg(title     ORDER BY ts DESC))[1]      AS sample_title,
        (array_agg(apply_url ORDER BY ts DESC))[1]      AS sample_url
      FROM filtered
      GROUP BY company_id
    ),
    scored AS (
      SELECT
        company_id,
        'software'::varchar AS role_family,
        (open_now + {weight_new} * new_last)::int AS score,
        json_build_object(
          'open_now',       open_now,
          'new_last_4w',    new_last,      -- keep the original key name the UI expects
          'sample_title',   sample_title,
          'evidence_urls',  COALESCE(ARRAY[NULLIF(sample_url, '')], ARRAY[]::text[])
        ) AS details_json
      FROM agg
      WHERE open_now > 0 OR new_last > 0
    )
    -- keep only the latest snapshot; simplest is delete + insert for the family
    DELETE FROM hiring_score WHERE lower(role_family) IN ('software','swe','sde');

    INSERT INTO hiring_score (company_id, role_family, score, details_json)
    SELECT company_id, role_family, score, details_json
    FROM scored;

    SELECT COUNT(*) AS n FROM hiring_score WHERE lower(role_family) IN ('software','swe','sde');
    """)

    # execute the whole block and fetch the final count
    with db.begin():
        res = db.execute(sql).fetchone()
    return int(res["n"]) if res and "n" in res else 0
