#!/bin/bash

# PostgreSQL connection strings
stacks_api_conn_string="postgresql://user1:password1@localhost:5432/stacks_api_db"
signer_metrics_api_conn_string="postgresql://user2:password2@localhost:5432/signer_metrics_db"

# Redis connection string
redis_url="redis://<user>:<password>@<host>:<port>/"  # e.g., redis://:password@localhost:6379/

# Fetch the last_redis_msg_id from the stacks API database
last_metrics_api_id=$(psql "$stacks_api_conn_string" -t -c "SELECT last_redis_msg_id FROM chain_tip;" | xargs)

# Fetch the sequence_id from the signer metrics database
last_stacks_api_id=$(psql "$signer_metrics_api_conn_string" -t -c "SELECT sequence_id FROM event_observer_requests ORDER BY id DESC LIMIT 1;" | xargs)

# Check if both values are numbers
if ! [[ "$last_metrics_api_id" =~ ^[0-9]+$ ]] || ! [[ "$last_stacks_api_id" =~ ^[0-9]+$ ]]; then
  echo "Error: One of the IDs is not a valid number."
  exit 1
fi

# Calculate the minimum of the two IDs
safeToTrim=$((last_metrics_api_id < last_stacks_api_id ? last_metrics_api_id : last_stacks_api_id))

# Run the Redis CLI XTRIM command using the Redis URL
echo "Trimming Redis stream to $safeToTrim using Redis URL: $redis_url"
redis-cli -u "$redis_url" XTRIM all $safeToTrim

# Success message
echo "Redis stream trimmed to $safeToTrim"
