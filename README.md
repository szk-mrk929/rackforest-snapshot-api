# Snapshot szolgáltatás

## Döntések

- Az állapotgép a `domain` csomagban él, tároló és HTTP nélkül: hét állapot, nyolc engedélyezett átmenet, törlés csak `ready`, `failed` és `error_deleting` állapotból, a `deleted` pedig nem foglal bérlőkvótát.
- A név és az azonosítók ugyanitt érvényesülnek (hossz, vezérlőkarakter, azonosítóban szóköz sem), az időbélyeg UTC, az illegális átmenet pedig `ErrInvalidState`. A JSON alak az API rétegé.
- A konfiguráció megtartja a meglévő neveket (`HTTP_ADDR`, `LOG_LEVEL`, `SHUTDOWN_TIMEOUT`, `WORKER_COUNT`, `WORKER_ATTEMPTS`, `WORKER_QUEUE_SIZE`), és kiegészül a bérlőkvótával (10), a storage-határidővel (30s), a retry alappal (200ms), valamint a mock 2–10 másodperces késleltetésével és 0,2-es hibaarányával; a csupasz szám továbbra is másodperc, a kiírt érvénytelen érték pedig elutasítás.
- A napló minden sorára a contextből másolja a kérésazonosítót; a kliens `X-Request-ID`-ja csak biztonságos karakterekkel marad meg, különben a szerver generál, és ugyanezt az azonosítót viszi tovább a worker.
- A memória-tároló egy mutex alatt számol kvótát és bérlőn belüli idempotencia-kulcsot, az idegen snapshot nem található, az illegális átmenetet a store utasítja el; a volume-zár kulcsa a `volume_id` lesz, nem a bérlő plusz a volume.
- A storage a `StorageBackend` mögött van: a véletlen mock a beállított késleltetéssel és hibaaránnyal fut, a sikeres create ismétlése siker, a determinisztikus tesztek pedig scriptelt backendet használnak.
- A worker FIFO sort futtat, a storage-hívás idejére tartja a globális N-es limitet, a volume-zárat a teljes retry-sorozatra (backoff közben is), és leállításkor a megszakított hívás után a snapshot `creating` vagy `deleting` állapotban marad.
- SIGINT és SIGTERM után az üzleti útvonal 503, a `/healthz` 200 marad, új job nem indul, a futó storage-hívás a `SHUTDOWN_TIMEOUT`-ig befejezheti, a sorban hagyott snapshot `pending` marad, a határidő lejártával megszakított hívás pedig `creating` vagy `deleting` állapotot hagy.
- Az API a generált modellek mögött a szabványos ServeMux: az `X-Tenant-ID` a `/healthz` kivételével kötelező, a hibaválasz `{"error":{"code","message"}}`. A 400 hibás kérés, a 404 ennek a bérlőnek nincs ilyen snapshotja (az idegen is ide tartozik), a 409 kvóta, rossz állapot vagy idempotencia-ütközés, a 429 teli sor, a 503 leállás. Az első POST 202 és `pending`, ugyanaz a kulcs később 200 az aktuális állapottal, a DELETE 202 és azonnal `deleting`. A lista a store beszúrási sorrendjének fordítottja, tehát a legújabb elöl.
