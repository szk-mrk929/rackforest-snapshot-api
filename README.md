# Snapshot szolgáltatás

## Döntések

- Az állapotgép a `domain` csomagban él, tároló és HTTP nélkül: hét állapot, nyolc engedélyezett átmenet, törlés csak `ready`, `failed` és `error_deleting` állapotból, a `deleted` pedig nem foglal bérlőkvótát.
- A név és az azonosítók ugyanitt érvényesülnek (hossz, vezérlőkarakter, azonosítóban szóköz sem), az időbélyeg UTC, az illegális átmenet pedig `ErrInvalidState`. A JSON alak az API rétegé.
- A konfiguráció megtartja a meglévő neveket (`HTTP_ADDR`, `LOG_LEVEL`, `SHUTDOWN_TIMEOUT`, `WORKER_COUNT`, `WORKER_ATTEMPTS`, `WORKER_QUEUE_SIZE`), és kiegészül a bérlőkvótával (10), a storage-határidővel (30s), a retry alappal (200ms), valamint a mock 2–10 másodperces késleltetésével és 0,2-es hibaarányával; a csupasz szám továbbra is másodperc, a kiírt érvénytelen érték pedig elutasítás.
- A napló minden sorára a contextből másolja a kérésazonosítót; a kliens `X-Request-ID`-ja csak biztonságos karakterekkel marad meg, különben a szerver generál, és ugyanezt az azonosítót viszi tovább a worker.
