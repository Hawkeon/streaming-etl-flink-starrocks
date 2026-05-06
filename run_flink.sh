#!/bin/bash
cd /opt/flink

# First run init-tables.sql to create fluss source tables
./bin/sql-client.sh -f /opt/flink/init/init-tables.sql << 'EOF'
-- This won't work because sql-client.sh -f doesn't support stdin
-- We need a different approach
EOF

# Actually let's run the flink-to-starrocks in the same session as init
# First create a combined script
cat /opt/flink/init/init-tables.sql /opt/flink/init/flink-to-starrocks.sql > /tmp/combined.sql
./bin/sql-client.sh -f /tmp/combined.sql