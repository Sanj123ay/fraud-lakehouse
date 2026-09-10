#!/usr/bin/env bash
# Start the local Airflow control plane and guarantee a usable UI admin.
set -euo pipefail

username="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
password="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
firstname="${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}"
lastname="${_AIRFLOW_WWW_USER_LASTNAME:-User}"
email="${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}"

echo "Migrating the Airflow metadata database..."
airflow db migrate

create_admin() {
  airflow users create \
    --username "${username}" \
    --password "${password}" \
    --firstname "${firstname}" \
    --lastname "${lastname}" \
    --role Admin \
    --email "${email}"
}

echo "Ensuring Airflow UI user '${username}' exists..."
if create_admin; then
  echo "Created Airflow UI admin '${username}'."
else
  # Airflow 2.8 has create/delete but no non-interactive reset-password command.
  # Recreate only this local UI account; DAG and task metadata remain untouched.
  echo "User '${username}' already exists; recreating it with configured credentials."
  airflow users delete --username "${username}"
  create_admin
  echo "Recreated Airflow UI admin '${username}'."
fi

echo "Starting Airflow webserver on port 8080 and scheduler..."
airflow webserver &
exec airflow scheduler
