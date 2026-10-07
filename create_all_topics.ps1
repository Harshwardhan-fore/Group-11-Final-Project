# ---------------------------------------------------------------------------
# FMCG FINAL PROJECT - create every Kafka topic for all three modules
# Run from PowerShell:   .\create_all_topics.ps1
#
# 10 data topics + 1 dead-letter topic.
# Partitions = 3, Replication = 1 (single broker), Retention = 7 days.
# ---------------------------------------------------------------------------

$BOOTSTRAP = "localhost:9092"
$PARTITIONS = 3
$REPLICATION = 1
$RETENTION = 604800000   # 7 days

$topics = @(
  # Manufacturing + Sales  (harshwardhan_a2)
  "fmcg.production.batch",
  "fmcg.machine.telemetry",
  "fmcg.quality.result",
  "fmcg.order.placed",
  "fmcg.sales.transaction",
  # Logistics  (hritik_a2)
  "fmcg.shipment.dispatch",
  "fmcg.shipment.delivery",
  "fmcg.warehouse.stock",
  "fmcg.warehouse.return",
  # E-commerce  (preeti_a2)
  "raw.events",
  "raw.events.dlq"
)

Write-Host "Creating $($topics.Count) topics on $BOOTSTRAP" -ForegroundColor Cyan

foreach ($t in $topics) {
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
    --bootstrap-server $BOOTSTRAP --topic $t `
    --partitions $PARTITIONS --replication-factor $REPLICATION `
    --config retention.ms=$RETENTION 2>&1 | Where-Object { $_ -notmatch "WARNING" }
}

Write-Host ""
Write-Host "Topics now on the broker:" -ForegroundColor Cyan
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP |
  Where-Object { $_ -match "^(fmcg|raw)\." } | Sort-Object

Write-Host ""
Write-Host "Expected: 11 topics. Count above should match." -ForegroundColor Yellow
