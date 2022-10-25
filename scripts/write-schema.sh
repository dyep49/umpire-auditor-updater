#!/bin/zsh

PGPASSWORD=$umpire_auditor_psql_password psql -h $db_host -U $db_user -f ./../database/schema.sql $db_name
