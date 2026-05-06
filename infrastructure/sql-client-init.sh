#!/usr/bin/bash
# Copy Fluss + StarRocks connector JARs from host lib dir to Flink lib
# Then start sql-client as normal
set -e

LIB_DIR="/tmp/lib"
FLINK_DIR="/opt/flink"

echo "[init] Waiting for Flink lib dir..."
while [ ! -d "$FLINK_DIR/lib" ]; do sleep 1; done

if [ -d "$LIB_DIR" ] && [ "$(ls -A $LIB_DIR 2>/dev/null)" ]; then
    echo "[init] Copying JARs from $LIB_DIR to $FLINK_DIR/lib ..."
    cp -v "$LIB_DIR"/*.jar "$FLINK_DIR/lib/" 2>/dev/null || true
    chown -v flink:flink "$FLINK_DIR/lib"/*.jar 2>/dev/null || true
    echo "[init] JARs copied:"
    ls -lh "$FLINK_DIR/lib/" | grep -E "fluss|starrocks"
else
    echo "[init] $LIB_DIR empty/missing, JARs may already exist"
fi

# S3 fs plugin
mkdir -p "$FLINK_DIR/plugins/s3-fs-hadoop"
mv -v "$FLINK_DIR/lib"/flink-s3-fs-hadoop-*.jar "$FLINK_DIR/plugins/s3-fs-hadoop/" 2>/dev/null || true
chown -v flink:flink "$FLINK_DIR/plugins/s3-fs-hadoop"/*.jar 2>/dev/null || true

echo "[init] Waiting for JobManager..."
while ! (echo > /dev/tcp/jobmanager/8081) >/dev/null 2>&1; do sleep 1; done
echo "[init] JobManager is up, starting SQL client..."

exec /docker-entrypoint.sh bin/sql-client.sh
tail -f /dev/null