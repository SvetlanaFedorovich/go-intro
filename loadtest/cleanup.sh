#!/usr/bin/env bash
set -euo pipefail

PAYLOAD='{"user":"LoadTest","age":31,"email":"loadtest@example.com"}'
PAYLOAD_HASH="$(
	PAYLOAD="$PAYLOAD" python3 -c \
		'import hashlib, os; print(hashlib.sha256(os.environ["PAYLOAD"].encode()).hexdigest())'
)"

"${CONTAINER_ENGINE:-podman}" exec postgres psql \
	-v ON_ERROR_STOP=1 \
	-U "${POSTGRES_USER:-postgres}" \
	-d "${POSTGRES_DB:-test}" \
	-c "DELETE FROM public.\"data\" WHERE email = 'loadtest@example.com';
	    DELETE FROM public.processed_events WHERE payload_hash = '$PAYLOAD_HASH';"
