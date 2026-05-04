# Silver Daily Tables Review

## Reviewed Items
- Verify whether key metrics—**Users**, **Pageviews**, and **Sessions**—from:
  - `SILVER_DAILY_KPI_SUMMARY`
  - `SILVER_DAILY_CONTENT_SUMMARY`  
  are aligned with metrics pulled from `SILVER_EVENT_DIM`.

## Purpose
The `SILVER_DAILY_KPI_SUMMARY` table is designed to reduce the volume of data queried by the Tableau Topline dashboard:
- From **6,212,642,623 rows** (for 3 months)
- To **200,131,526 rows (3%)** 

Note: SILVER_DAILY_CONTENT_SUMMARY's row count for 3 months: 256,796,716 (4%)

This reduction in data volume is critical to ensuring the dashboard remains **functional and responsive**.

---

## Review Log

### Review on Apr 10, 2026
- **Changes reviewed**
  - `UTM_REFERRER_SIMPLIFIED` logic  
  - `REGION` and `CONTINENT` variables  
  - `SESSIONS_FOR_USERTYPE_ALL` logic
- The `SILVER_EVENT_DIM` and summary tables are aligned **except for two documented exceptions** (see notes below).
- **User count discrepancy**
  - Discrepancy observed is **up to 1%**
- **Comparison with Google Analytics**
  - Users and Pageviews: **<5% difference**
  - Sessions: **~15% difference**
- **Other notes**
  - There are many rows where `SS_COUNTRY_ID` or `FIRST_UTM_REFERRER` is `NULL`.  
    The daily table remains aligned with the star schema table despite these null values.

---

### Review on Dec 9, 2025
- Review conducted following updates to logic:
  - User count logic changed to `COALESCE(user_dim_id, anonymous_user_id)`
  - Bot filter applied
- **Daily KPI**
  - Items **1–7** and **D1, D2** passed
- **Monthly KPI**
  - Items **1–7, 11, 14** passed
- **Daily Content**
  - Items **A1–A9** and **B1–B4** passed
- **Monthly KPI**
  - Items **1–8** passed

---

### Review on Nov 4, 2025
- Data from **Jan 1–31, 2025** was used for review.
- Metrics in Tableau (`TOPLINE_SILVER_V1` file and summary tables) are aligned with figures pulled from `SILVER_EVENT_DIM`.
- **Exception: Session Count**
  - The logic assumes `USER_DIM_ID` is not `NULL` when `application_authenticated_state` is `"registered"` or `"subscriber"`.
  - In rare cases, `USER_DIM_ID` is recorded as `NULL` even when the user is authenticated.
- **Impact**
  - Session counts by user type may differ slightly for `"registered"` and `"subscriber"` users.
  - Discrepancy is **<1%**, and results are considered aligned when session count differences remain below this threshold.
