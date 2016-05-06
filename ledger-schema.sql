--- Schema for ledgerSQL

CREATE TABLE data_sources (
   source_id serial primary key,
   filename text,
   loaded_on timestamptz not null,
   first_loaded timestamptz not null
);
comment on table data_sources is 'Identifies sources of data';
comment on column data_sources.source_id is 'internal ID of source';
comment on column data_sources.filename is 'filename';
comment on column data_sources.loaded_on is 'when was this data loaded?';
comment on column data_sources.first_loaded is 'when was this data first loaded?';

create index ds_fn on data_sources(filename);

create table labels (
   label_id serial primary key,
   label text not null unique
);

comment on table labels is 'Allows attaching logical labels to data sources';
comment on column labels.label_id is 'Internal ID';
comment on column labels.label is 'Unique label';

create table source_labels (
   source_id integer not null references data_sources(source_id) on delete cascade,
   label_id integer not null references labels(label_id) on delete restrict,
   primary key (source_id, label_id)
);
create index slbl_label on source_labels(label_id);

comment on table source_labels is 'Associates labels with data sources';
comment on column source_labels.source_id is 'Which data source?';
comment on column source_labels.label_id is 'Which label?';

CREATE TABLE ledger (
    source_id integer references data_sources(source_id),
    xtn_date date,
    checknum text,
    note text,
    account text,
    commodity text,
    amount numeric,
    tags text,
    xtn_month date,
    xtn_year date,
    virtual boolean,
    xtn_id integer,
    cleared boolean,
    cost numeric
);

CREATE INDEX ledger_account_source ON ledger USING btree (source_id);
CREATE INDEX ledger_account_index ON ledger USING btree (account);
CREATE INDEX ledger_commodity_index ON ledger USING btree (commodity);
CREATE INDEX ledger_note_index ON ledger USING btree (note);
CREATE INDEX ledger_tags_index ON ledger USING btree (tags);
CREATE INDEX ledger_virtual_index ON ledger USING btree (virtual);
CREATE INDEX ledger_xtn_date_index ON ledger USING btree (xtn_date);
CREATE INDEX ledger_xtn_month_index ON ledger USING btree (xtn_month);
CREATE INDEX ledger_xtn_year_index ON ledger USING btree (xtn_year);

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
	SELECT (select xtn_date from public.ledger order by xtn_date limit 1) + SEQUENCE.DAY AS datum
	FROM generate_series(-366, (select (select xtn_date from ledger order by xtn_date desc limit 1) - (select xtn_date from ledger order by xtn_date asc limit 1) + 366*2)) AS SEQUENCE(DAY)
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

--  - I wonder if there should be some sort of
--    multi-versioning, so that if we re-load a data
--    file, we capture something sorta like DNS serial
--    number temporality, so we could audit the
--    progression of the data file over time...

