CREATE TABLE IF NOT EXISTS public."data" (
	id            serial4 NOT NULL,
	event_id      text    NOT NULL,
	payload_hash  text    NOT NULL,
	"user"        text    NOT NULL,
	age           int2    NOT NULL,
	email         text    NOT NULL,

	PRIMARY KEY (id),
	CONSTRAINT data_event_id_key UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS public.processed_events (
	event_id      text        NOT NULL,
	payload_hash  text        NOT NULL,
	topic         text        NOT NULL,
	partition     int4        NOT NULL,
	"offset"      int8        NOT NULL,
	processed_at  timestamptz NOT NULL DEFAULT now(),

	PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS processed_events_processed_at_idx
	ON public.processed_events (processed_at);
