'''

TEST_LOCATION = "HP"


# Homepage page path = "/"

# Sessions with a LOCATION visit
# By device - Desktop (Web), Mobile (Web), App

# Parameters
anchor_date = current_date() - 2

# Input tables
warehouse.bq_sessions_next_uk_app
warehouse.bq_sessions_next_uk
warehouse.bq_pages_next_uk
warehouse.uk_pf
[ad assignments by location table]


# Queries (LOCATION = "HP")
HP_Hit=spark.sql("""
select
    r.account_number,
    a.UniqueVisitID,
    Device,
    date as session_date,
    min(TransactionRevenue) as RPS
from (
    select distinct UniqueVisitID, TransactionRevenue, RPID, Device, date
    from warehouse.bq_sessions_next_uk
    where date=current_date()-2 /* and device='Desktop'*/) a
    inner join (
        select UniqueVisitID, min(FirstTimestamp) as FirstHit
        from warehouse.bq_pages_next_uk
        where PagePath = '/'
        and date=current_date()-2
        group by all) b
on a.UniqueVisitID=b.UniqueVisitID
inner join warehouse.rpid_with_accounts r
on a.RPID=r.roamingprofileid
group by all
""")

HP_Next_Page=spark.sql("""
select distinct  a.UniqueVisitID, NextPagePath
from (
    select distinct UniqueVisitID, RPID
    from warehouse.bq_sessions_next_uk
    where date=current_date()-2) a
    inner join (
        select distinct UniqueVisitID, NextPagePath
        from warehouse.bq_pages_next_uk
        where PagePath = '/'
        and date=current_date()-2) b
on a.UniqueVisitID=b.UniqueVisitID
group by all
""")


# When partial value of session required (e.g. OC, SB)
# Filter on session value after timestamp of OC/SB hit


# Tables - Temp for the run (overwritten each time)
LOCATION_hit - account_number, UniqueVisitID, Device, session_date, RPS
LOCATION_account - account_number
    (select distinct account_number from LOACTION_hit)
LOCATION_next_page - UniqueVisitID, NextPagePath


# Union web and app account temp tables (ensure distinct)
#   Join actual MASID assignment from uk_pf
#       (where rundate = anchor_date - assigned after midnight)
#   Join onto LOCATION (best/random/ctrl) assignment
#       (where rundate = anchor_date - 1 - assigned before midnight)
#   Union hit data across devices and append

# Get results history file
# - append/overwrite where session date >= current_date() - 2
# - Overwrite history file with new results appended


# repeat all for anchor_date = current_date() - 1 (MASID retreival - 2)
# Export overall results to BQ for dashboard

# Calculate ad-level results
# Export to BQ for dashboard

'''
