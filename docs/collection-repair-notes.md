# Financial collection repair findings

The uploaded diagnostic sample identified several distinct problems.

| Finding | Change | Remaining limitation |
|---|---|---|
| Four sampled 404 URLs ended in `/xbrl/-` | Reject missing-attachment sentinels before requests; quarantine existing queue rows without deletion | This does not supply an absent attachment |
| AKSHOPTFBR and GARFIBRES source files contain valid quarters and conflicting cumulative-context dates | Validate declared contexts against their own reporting-date facts and select the actual validated duration | Conflicting cumulative facts remain unavailable |
| A Half yearly source label accompanied three-month data | Select quarter or half-year using validated entity totals rather than the label alone | Genuine six-month reports remain six-month reports |
| Older WEB XML files reference missing contexts | Preserve bytes and report unavailable core facts/contexts | No context or numeric values are invented |
| The financial queue ran before independent datasets | Process independent download/discovery jobs first | Free archives can still have other historical gaps |

The revised parser extracted the quarter values directly from the two actual
source files: AKSHOPTFBR revenue 25.8913 crore and PAT 3.5279 crore; GARFIBRES
revenue 328.2982 crore and PAT 52.3941 crore. These checks verify extraction from
those documents, not company-wide historical completeness or strategy returns.

Regressions cover placeholder migration, no network requests for sentinels,
contradictory cumulative periods, genuine half-year reports, preservation of
missing-context failures, no selection of unexplained conflicting totals,
reported 404 gaps and independent data collection before document failures.

The diagnostic and coverage files under `diagnostics/` are user-uploaded
snapshots from before this repair. They are intentionally not rewritten as if
the repaired collector had already run on the user's computer.
