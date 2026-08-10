package store

const sqlInsertDataOnce = `
INSERT INTO public."data" (event_id, payload_hash, "user", age, email)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (event_id) DO NOTHING`

const sqlDataPayloadHash = `
SELECT payload_hash
FROM public."data"
WHERE event_id = $1`

const sqlRecordProcessedEvent = `
INSERT INTO public.processed_events (event_id, payload_hash, topic, partition, "offset", processed_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (event_id) DO UPDATE SET
	payload_hash = EXCLUDED.payload_hash,
	topic = EXCLUDED.topic,
	partition = EXCLUDED.partition,
	"offset" = EXCLUDED."offset",
	processed_at = EXCLUDED.processed_at`

const sqlCleanupProcessed = `
DELETE FROM public.processed_events
WHERE processed_at < $1`
