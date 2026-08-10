# --- build stage ---
# Полное имя образа (docker.io/library/...) - чтобы rootless Podman не
# переспрашивал реестр из-за unqualified-search-registries.
FROM docker.io/library/golang:1.26 AS build
WORKDIR /src

# сначала только модули - слой кэшируется, пока go.mod/go.sum не менялись
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 - статический бинарь, чтобы работал в distroless/scratch
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/worker ./cmd/worker

# --- runtime stage ---
# distroless static: без shell и пакетного менеджера, минимальная поверхность атаки
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api /api
COPY --from=build /out/worker /worker
USER nonroot:nonroot
