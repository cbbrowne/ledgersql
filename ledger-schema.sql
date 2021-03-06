--- Schema for ledgerSQL

CREATE TABLE ledger_sources (
   source_id serial primary key,
   ledger_label text not null unique,
   first_loaded timestamptz not null default now()
);
comment on table ledger_sources is 'Identifies sources of data';
comment on column ledger_sources.source_id is 'internal ID of source';
comment on column ledger_sources.ledger_label is 'unique ledger label';
comment on column ledger_sources.first_loaded is 'when was this source initiated?';

create table ledger_versions (
  source_id integer references ledger_sources(source_id) on delete cascade,
  created_on timestamptz not null default now(),
  ledger_version serial not null,
  primary key (ledger_version)
);
create index lv_version on ledger_versions(source_id, ledger_version);

comment on table ledger_versions is 'Every time a ledger is loaded, a new version will be established';
comment on column ledger_versions.source_id is 'Source ledger';
comment on column ledger_versions.created_on is 'When was new version established?';
comment on column ledger_versions.ledger_version is 'ID of this ledger version';

create table ledger_metadata (
   source_id integer references ledger_sources(source_id) on delete cascade,
   ledger_version integer references ledger_versions(ledger_version) on delete cascade,
   metadata_label text,
   metadata_value text,
   primary key (ledger_version, metadata_label)
);

comment on table ledger_metadata is 'Metadata for a ledger version';
comment on column ledger_metadata.source_id is 'Reference to ledger source';
comment on column ledger_metadata.ledger_version is 'Reference to specific version';
comment on column ledger_metadata.metadata_label is 'Metadata label';
comment on column ledger_metadata.metadata_value is 'Metadata value';

create table ledger_stats (
   source_id integer references ledger_sources(source_id) on delete cascade,
   ledger_version integer references ledger_versions(ledger_version) on delete cascade,
   stats_label text,
   stats_value integer,
   primary key (ledger_version, stats_label)
);

comment on table ledger_stats is 'Statistics about ledger data';
comment on column ledger_stats.source_id is 'Reference to ledger source';
comment on column ledger_stats.ledger_version is 'Reference to specific version';
comment on column ledger_stats.stats_label is 'Statistical label';
comment on column ledger_stats.stats_value is 'Value of statistic';

create table ledger_content (
   source_id integer references ledger_sources(source_id) on delete cascade,
   version_from integer references ledger_versions(ledger_version) on delete cascade,
   version_to integer references ledger_versions(ledger_version) on delete cascade,
   constraint version_ordering check (version_to is null or version_to > version_from),
   ledger_line integer,
   ledger_entry integer,
   ledger_date date,
   ledger_payee text,
   ledger_account text,
   ledger_commodity text,
   ledger_amount numeric(14,2),
   ledger_cleared boolean,
   ledger_virtual boolean,
   ledger_note text,
   ledger_cost numeric(14,2),
   ledger_code text,
   primary key (source_id, version_from, ledger_line, ledger_entry)
);
create index lc_active_content on ledger_content(source_id) where (version_to is null);
create index lc_date on ledger_content(ledger_date);

--- ledger-web did a data-warehousey thing of having a view that
--- generated lists of all the dates found in the data sets,
--- generating views for years, months, and days.

--- Seems a neat idea to maintain something similar here, but instead
--- via generating tables after ledger processing.

-- a) Want to have lists of all the dates in the data sets
--
-- b) Also, let's have lists of all the dates available in the
--    date ranges indicated.  Thus, each year from earliest to
--    latest, each month in those ranges, and each day in
--    those ranges

-- c) It's common for data warehousing to have date dimension
--    tables that indicate, for each date that can be in the
--    data set, various characteristics such as
--     - day of week
--     - day of month
--     - is it a holiday in a relevant calendar?

--  Note that the view was pulled from PostgreSQL wiki
--  https://wiki.postgresql.org/wiki/Date_and_Time_dimensions

create or replace view v_dates_around_relevant_data as
SELECT
	datum AS DATE,
	EXTRACT(YEAR FROM datum) AS YEAR,
	EXTRACT(MONTH FROM datum) AS MONTH,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	EXTRACT(DAY FROM datum) AS DAY,
	EXTRACT(doy FROM datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	EXTRACT(week FROM datum) AS CalendarWeek,
	to_char(datum, 'dd. mm. yyyy') AS FormattedDate,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	-- Fixed holidays 
        -- for America
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END
		AS AmericanHoliday,
        -- for Austria
	CASE WHEN to_char(datum, 'MMDD') IN 
		('0101', '0106', '0501', '0815', '1101', '1208', '1225', '1226') 
		THEN 'Holiday' ELSE 'No holiday' END 
		AS AustrianHoliday,
        -- for Canada
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0701', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END 
		AS CanadianHoliday,
	-- Some periods of the year, adjust for your organisation and country
	CASE WHEN to_char(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
		ELSE 'Normal' END
		AS Period,
	-- ISO start and end of the week of this date
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER AS CWStart,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER AS CWEnd,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS MonthStart,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL AS MonthEnd
FROM (
        -- start 366 days before the earliest date, and
	-- extend to a bit over a year after the latest date
	SELECT (select ledger_date from public.ledger_content order by ledger_date limit 1) + SEQUENCE.DAY AS datum
	FROM generate_series(-366, (select (select ledger_date from ledger_content order by ledger_date desc limit 1) - (select ledger_date from ledger_content order by ledger_date asc limit 1) + 366*2)) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ
;

create materialized view date_dimension as
  select * from v_dates_around_relevant_data;

create index dd_date on date_dimension(date);
create index dd_year on date_dimension(year);
create index dd_month on date_dimension(month);
create index dd_monthname on date_dimension(monthname);
create index dd_day on date_dimension(day);

-- And we should have something that periodically runs...
refresh materialized view date_dimension;
-- for instance, any time a new batch is loaded into public.ledger
-- we should refresh the dimension, maybe?

-- There should be several representations of data...

--  - As near-raw input, where the point is to know
--    about where it came from.  Not to be referenced
--    directly for accounting queries; all about
--    data /provenance/...

--  - As aggregated data, which will look somewhat
--    data-warehouse-y.  It'll reference the "raw" data,
--    but "cook" it into usable form for queries

create view v_latest_ledger as
   select ls.ledger_label, lv.created_on, lv.ledger_version,
   lc.ledger_line, lc.ledger_entry, lc.ledger_date, lc.ledger_payee, lc.ledger_account,
   lc.ledger_commodity, lc.ledger_amount, lc.ledger_cleared, lc.ledger_virtual,
   lc.ledger_note, lc.ledger_cost, lc.ledger_code
from
   ledger_sources ls, ledger_versions lv, ledger_content lc
where
   ls.source_id = lv.source_id and 
   ls.source_id = lc.source_id and
   lc.version_to is null;

create materialized view latest_ledger as
   select * from v_latest_ledger;

create index ll_label on latest_ledger(ledger_label);
create index ll_line on latest_ledger(ledger_line);
create index ll_account on latest_ledger(ledger_account);
create index ll_date on latest_ledger(ledger_date);

create view v_monthly_ledger_summary as
   select ls.ledger_label, date_trunc('month', ledger_date) as ledger_month,
   ledger_account, ledger_commodity, sum(ledger_amount) as ledger_amount,
   ledger_cleared, ledger_virtual
from v_latest_ledger ls
   group by ledger_label, ledger_month, ledger_account, ledger_commodity, ledger_cleared, ledger_virtual;

create materialized view monthly_ledger_summary as
   select * from v_monthly_ledger_summary;

create index ml_label on monthly_ledger_summary(ledger_label);
create index ml_account on monthly_ledger_summary(ledger_account);
create index ml_month on monthly_ledger_summary(ledger_month);

create or replace view v_monthly_ledger_balances as
   select ledger_label, ledger_month, ledger_account, ledger_commodity, ledger_cleared, ledger_virtual,
   sum(ledger_amount) over (partition by ledger_label, ledger_account order by ledger_month) as ledger_balance
from v_monthly_ledger_summary;

create materialized view monthly_ledger_balances as
   select * from v_monthly_ledger_balances;
