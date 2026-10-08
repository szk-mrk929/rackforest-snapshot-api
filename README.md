# Snapshot szolgáltatás

## Döntések

- Az állapotgép a `domain` csomagban él, tároló és HTTP nélkül: hét állapot, nyolc engedélyezett átmenet, törlés csak `ready`, `failed` és `error_deleting` állapotból, a `deleted` pedig nem foglal bérlőkvótát.
