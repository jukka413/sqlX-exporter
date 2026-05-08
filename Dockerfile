# =============================================================================
# Stage 1 — builder
# =============================================================================
FROM golang:1.26-alpine AS builder

# Устанавливаем ca-certificates и tzdata:
# - ca-certificates нужны для TLS-соединений с БД (особенно Oracle/MSSQL)
# - tzdata нужен для time.LoadLocation() — schedule.timezone в конфиге
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /build

# Копируем go.mod и go.sum отдельно чтобы использовать кеш слоёв Docker.
# Зависимости пересчитываются только если изменился go.mod/go.sum,
# но не при каждом изменении исходного кода.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

# CGO_ENABLED=0 — статическая сборка без libc, бинарник запустится в scratch/distroless
# -ldflags "-s -w" — убираем debug-символы и DWARF, уменьшает размер бинарника ~30%
# -trimpath — убираем локальные пути из бинарника (reproducible builds + безопасность)
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /sqlx-exporter \
    .

# =============================================================================
# Stage 2 — final image
# =============================================================================
# distroless/static — минимальный образ без shell и пакетного менеджера:
# - нет bash/sh → меньше attack surface
# - нет лишних библиотек → меньше CVE
# - размер финального образа ~10-15 МБ
FROM gcr.io/distroless/static-debian12:nonroot

# Копируем ca-certificates и tzdata из builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder /sqlx-exporter /sqlx-exporter

# nonroot образ уже запускается от пользователя 65532 (nonroot)
# Дополнительно явно указываем для ясности и совместимости с k8s securityContext
USER 65532:65532

# GOGC=50 — запускать GC вдвое чаще чем по умолчанию (default=100).
# Уменьшает пиковое потребление памяти за счёт небольшого роста CPU (~1-2%).
# Для экспортёра с малым количеством горутин это оптимальный баланс.
# GOMEMLIMIT задаёт мягкий лимит heap — Go будет агрессивнее запускать GC
# когда приближается к лимиту. Установи чуть ниже resources.limits.memory.
ENV GOGC=50
ENV GOMEMLIMIT=55MiB
# MADV_DONTNEED — немедленно возвращать свободные страницы ядру.
# Без этого Go использует MADV_FREE (default на Linux) — страницы остаются
# в RSS пока ядро не заберёт их под давлением. С MADV_DONTNEED RSS
# точно отражает реальное потребление, скачков не будет.
ENV GODEBUG=madvdontneed=1

EXPOSE 2112

# config.yaml монтируется снаружи (ConfigMap в k8s или -v в docker run)
# Путь задаётся флагом --config, дефолт ./config.yaml
ENTRYPOINT ["/sqlx-exporter"]
CMD ["--config=/etc/sqlx-exporter/config.yaml"]
