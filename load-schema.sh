#!/bin/bash
URI=$1
psql -d $URI -f ledger-schema.sql
