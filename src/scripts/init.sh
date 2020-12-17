#!/usr/bin/env bash

case "$1" in
"webserver")
  airflow initdb

  airflow scheduler &
  exec airflow webserver

  user=$(airflow list_users | grep leochen)
  if [ -z "$user" ]; then
    airflow create_user -r Admin -u leochen -e event10342012@gmail.com -f Leo -l Chen -p leochen
  fi
  ;;

*)
  exec "$@"
  ;;
esac
