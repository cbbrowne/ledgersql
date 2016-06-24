#!/bin/bash
DBURI=$1
LEDGERLABEL=$2
LEDGERFILE=$3
TEMPLEDGERDATA=`mktemp /tmp/ledger-data.XXXXXXXXXX`
TEMPSQL=`mktemp /tmp/ledger-sql.XXXXXXXXXX`
TEMPSTATS=`mktemp /tmp/ledger-stats.XXXXXXXXX`

LEDGERFMT="%(quoted(xact.beg_line)),%(quoted(date)),%(quoted(payee)),%(quoted(account)),%(quoted(commodity)),%(quoted(quantity(scrub(display_amount)))),%(quoted(cleared)),%(quoted(virtual)),%(quoted(join(note | xact.note))),%(quoted(cost)),%(quoted(code))\n"

echo "Processing ledger label [${LEDGERLABEL}]
Loading into database with URI:[${DBURI}]
Ledger file:[${LEDGERFILE}]"

lhost=`hostname`
luser=`whoami`
ldate=`date +"%Y-%m-%d %H:%M:%S %z"`
echo "Supplemental metadata:
 - hostname=[${lhost}]
 - user=[${luser}]
 - load date=[${ldate}]"

if [ -e ${LEDGERFILE} ]; then
	ledger --file ${LEDGERFILE} --format="${LEDGERFMT}" reg > ${TEMPLEDGERDATA}
	echo "Pulled ledger file ${LEDGERFILE} into ${TEMPLEDGERDATA} in CSV format"
	LEDGERSIZE=`wc -l - < ${TEMPLEDGERDATA}`
	echo "Size: ${LEDGERSIZE} lines"
else
	echo "No such ledger file as [${LEDGERFILE}]"
	exit 1
fi

# Some load stats will get dumped into TEMPSTATS
echo "
begin;
create temp table t_raw_ledger (tx_line text, tx_date text, tx_payee text, tx_account text, tx_commodity text, tx_amount text, tx_cleared text, tx_virtual text, tx_note text, tx_cost text, tx_code text);
\\copy t_raw_ledger (tx_line, tx_date, tx_payee, tx_account, tx_commodity, tx_amount, tx_cleared, tx_virtual, tx_note, tx_cost, tx_code) from '${TEMPLEDGERDATA}' with csv;

create temp sequence t_line_seq;

create temp table t_less_raw as
  select tx_line::integer as tx_line, nextval('t_line_seq') as tx_entry, tx_date::date as tx_date, tx_payee, tx_account, tx_commodity, btrim(tx_amount, '$')::numeric(12,2) as tx_amount, tx_cleared::boolean, tx_virtual::boolean, tx_note, btrim(tx_cost, '$')::numeric(12,2) as tx_cost, tx_code from t_raw_ledger;
create temp table t_metadata (label text, value text);
insert into t_metadata (label, value) values ('hostname', '${lhost}'), ('user', '${luser}'), ('load date', '${ldate}'), ('filename', '${LEDGERFILE}'), ('label', '${LEDGERLABEL}');

do \$\$
begin
   if not exists (select 1 from ledger_sources where ledger_label='${LEDGERLABEL}') then
      insert into ledger_sources (ledger_label) values ('${LEDGERLABEL}');
   end if;
end \$\$ language plpgsql;

select source_id into temp table t_source from ledger_sources where ledger_label = '${LEDGERLABEL}';

create temp table unmodified_content as
  select tx_line, tx_entry, tx_date, tx_payee, tx_account, tx_commodity, tx_amount, tx_cleared, tx_virtual, tx_note, tx_cost, tx_code from t_less_raw
  where exists (select 1 from ledger_content
     where source_id = (select source_id from t_source) and version_to is null
      and tx_line = ledger_line and tx_date = ledger_date and tx_payee = ledger_payee and tx_account = ledger_account and tx_commodity = ledger_commodity and tx_amount = ledger_amount and tx_cleared = ledger_cleared and tx_virtual = ledger_virtual and tx_note = ledger_note and tx_cost = ledger_cost and tx_code = ledger_code);

create temp table moved_content as
  select tx_line, tx_entry, tx_date, tx_payee, tx_account, tx_commodity, tx_amount, tx_cleared, tx_virtual, tx_note, tx_cost, tx_code from t_less_raw
  where exists (select 1 from ledger_content
     where source_id = (select source_id from t_source) and version_to is null
      and tx_line <> ledger_line and tx_date = ledger_date and tx_payee = ledger_payee and tx_account = ledger_account and tx_commodity = ledger_commodity and tx_amount = ledger_amount and tx_cleared = ledger_cleared and tx_virtual = ledger_virtual and tx_note = ledger_note and tx_cost = ledger_cost and tx_code = ledger_code);
alter table moved_content add column ledger_line integer;

update moved_content
  set ledger_line = (select min(ledger_line) from ledger_content
     where source_id = (select source_id from t_source) and version_to is null
      and (tx_line <> ledger_line or tx_entry <> ledger_entry) and tx_date = ledger_date and tx_payee = ledger_payee and tx_account = ledger_account and tx_commodity = ledger_commodity and tx_amount = ledger_amount and tx_cleared = ledger_cleared and tx_virtual = ledger_virtual and tx_note = ledger_note and tx_cost = ledger_cost and tx_code = ledger_code);

create temp table new_content as
  select tx_line, tx_entry, tx_date, tx_payee, tx_account, tx_commodity, tx_amount, tx_cleared, tx_virtual, tx_note, tx_cost, tx_code from t_less_raw
  where not exists (select 1 from ledger_content
     where source_id = (select source_id from t_source) and version_to is null
       and tx_date = ledger_date and tx_payee = ledger_payee and tx_account = ledger_account and tx_commodity = ledger_commodity and tx_amount = ledger_amount and tx_cleared = ledger_cleared and tx_virtual = ledger_virtual and tx_note = ledger_note and tx_cost = ledger_cost and tx_code = ledger_code);  

create temp table removed_content as
  select ledger_line from ledger_content where
    version_to is null and source_id = (select source_id from t_source) and
    not exists (
     select 1 from t_less_raw
     where 
       tx_date = ledger_date and tx_payee = ledger_payee and tx_account = ledger_account and tx_commodity = ledger_commodity and tx_amount = ledger_amount and tx_cleared = ledger_cleared and tx_virtual = ledger_virtual and tx_note = ledger_note and tx_cost = ledger_cost and tx_code = ledger_code);

do \$\$
begin
   if exists (select 1 from moved_content) or exists (select 1 from new_content) or exists (select 1 from removed_content) then
      -- establish metadata
      insert into ledger_versions (source_id) select source_id from t_source;
      create temp table t_version as
      select source_id, max(ledger_version) as ledger_version from ledger_versions where source_id = (select source_id from t_source) group by 1;
      insert into ledger_metadata (source_id, ledger_version, metadata_label, metadata_value)
           select source_id, ledger_version, label, value from t_metadata, t_version;
      -- obsolesce old data
      update ledger_content set version_to = (select ledger_version from t_version)
         where source_id in (select source_id from t_source) and version_to is null and
         ledger_line in (select ledger_line from removed_content);
      update ledger_content set version_to = (select ledger_version from t_version)
         where source_id in (select source_id from t_source) and version_to is null and
         ledger_line in (select ledger_line from moved_content);
      -- add new data
      insert into ledger_content (source_id, version_from, ledger_line, ledger_entry, ledger_date, ledger_payee, ledger_account, ledger_commodity, ledger_amount, ledger_cleared, ledger_virtual, ledger_note, ledger_cost, ledger_code)
      select source_id, ledger_version, tx_line, tx_entry, tx_date, tx_payee, tx_account, tx_commodity, tx_amount, tx_cleared, tx_virtual, tx_note, tx_cost, tx_code from new_content, t_version;
   end if;
end \$\$ language plpgsql;


commit;
" > ${TEMPSQL}

psql --variable ON_ERROR_STOP=1 -d ${DBURI} -f ${TEMPSQL}
if [ $? -eq 0 ]; then
	echo "Loaded OK"
	# And add in reporting of statistics

	# Clear out temp files
	#rm -f $TEMPLEDGERDATA $TEMPSQL $TEMPSTATS
else
	echo "Problem loading SQL: rc=$?"
fi
