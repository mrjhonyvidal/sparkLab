#!/bin/bash
echo "============== psql ===================="
echo "\\dt : Describe current database"
echo "\\d [table] : Describe tables, views, sequences, index"
echo "\\c : Connect to a database"
echo "\\h : help with SQL commands"
echo "\\? : help with psql commands"
echo "\\q : quit"
echo "======================================"
docker exec -it postgres psql -U docker -d scalalab