#!/bin/bash
DBURI=$1
LEDGERFILE=$2
TEMPLEDGERDATA=`mktemp /tmp/ledger-data.XXXXXXXXXX`
TEMPSQL=`mktemp /tmp/ledger-sql.XXXXXXXXXX`
TEMPSTATS=`mktemp /tmp/ledger-stats.XXXXXXXXX`

LEDGERFMT="%(quoted(xact.beg_line)),%(quoted(date)),%(quoted(payee)),%(quoted(account)),%(quoted(commodity)),%(quoted(quantity(scrub(display_amount)))),%(quoted(cleared)),%(quoted(virtual)),%(quoted(join(note | xact.note))),%(quoted(cost))\n"

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
create temp table t_raw_ledger (tx_line text, tx_date text, tx_payee text, tx_account text, tx_commodity text, tx_amount text, tx_cleared text, tx_virtual text, tx_note text, tx_cost text);
\\copy t_raw_ledger (tx_line, tx_date, tx_payee, tx_account, tx_commodity, tx_amount, tx_cleared, tx_virtual, tx_note, tx_cost) from '${TEMPLEDGERDATA}' with csv;

create temp table t_less_raw as
  select tx_line::integer as tx_line, tx_date::date as date, tx_payee, tx_account, tx_commodity, btrim(tx_amount, '$')::numeric(12,2) as tx_amount, tx_cleared::boolean, tx_virtual::boolean, tx_note, btrim(tx_cost, '$')::numeric(12,2) as tx_cost from t_raw_ledger;
 
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


