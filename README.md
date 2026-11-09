# Data Engineering – pražská doprava

Týmový projekt pro předmět Data Engineering na VŠE v Praze.
Datová platforma v Microsoft Fabric pro fiktivního operátora pražské MHD
pokrývá načítání dat, uchování surových dat ve vrstvě Bronze a jejich čištění
a propojení ve vrstvě Silver.

## Tým

| Člen | Role |
| --- | --- |
| Martin Růžek | Tech Lead |
| Marie Anna Bibková | Data Ops a Governance |
| David Fridrich | Silver Engineer |
| Zuzana Staníčková | Ingest Engineer – Internal |
| Jakub Lisý | Ingest Engineer – External |

## Kontrola počtu řádků v Bronze

| Tabulka ve zdroji | Počet ve zdroji | Počet v Bronze | Výsledek |
| --- | ---: | ---: | --- |
| dbo.CESTUJICI | 61964 | 61964 | OK |
| dbo.CLAIM_REKLAMACE_POKUTY | 450 | 450 | OK |
| dbo.COLLECTION_VYMAHANI | 976 | 976 | OK |
| dbo.PLATBA | 4582 | 4582 | OK |
| dbo.PODNET_CESTUJICIHO | 809 | 809 | OK |
| dbo.POHLEDAVKA | 5000 | 5000 | OK |
| dbo.PREPRAVNI_KONTROLA | 5000 | 5000 | OK |
| dbo.REFUND | 147 | 147 | OK |
| dbo.REVIZOR | 300 | 300 | OK |

## Silver – pravidla čištění

Silver vrstva vzniká v notebooku `nb_silver` z každé Bronze tabulky (`bronze_x` → `silver_x`).
Tabulky se při každém běhu přepisují (overwrite), opakované spuštění tedy nevytváří duplicity.

### Pravidla pro všechny tabulky
- **Názvy sloupců:** převedeny na malá písmena s podtržítky; `LOADED_AT` přejmenován na `_loaded_at`.
- **Datové typy:** data jako `date` / `timestamp`, částky jako `decimal(10,2)`.
- **Texty:** oříznuty mezery na začátku a konci, prázdný text nahrazen hodnotou NULL.
- **Chybějící hodnoty:** ponechány jako NULL (neodhadujeme je). Výjimkou je chybějící klíč tabulky – takový řádek je vyřazen.
- **Duplicity:** podle klíče tabulky (první sloupec `*_sk`) se ponechá jen nejnovější záznam podle `created_at`.
- **Nesmyslné hodnoty:** datum nebo čas pozdější než čas načtení (tedy v budoucnosti) je nahrazen NULL. Výjimkou jsou sloupce platnosti (`valid_*`).

### Pravidla pro konkrétní tabulky
| Tabulka | Nalezená vada | Řešení |
|---|---|---|
| `silver_cestujici` | Datum narození před rokem 1910 (např. 1900-01-03) | Nahrazeno NULL |
| `silver_cestujici` | Jména začínající nepísmennými znaky (např. „- TETZNER“) | Znaky na začátku odstraněny |
| `silver_podnet_cestujiciho` | `incident_time` uložen jako `1970-01-01 hh:mm` | Převeden na čas ve formátu `HH:mm:ss` |
| `silver_claim_reklamace_pokuty` | `resolved_at` v budoucnosti | Nahrazeno NULL (obecné pravidlo) |
| `silver_platba` | `payment_timestamp` a `created_at` v budoucnosti | Nahrazeno NULL (obecné pravidlo) |

### Výsledek
| Tabulka | Klíč | Počet řádků v Silver |
|---|---|---:|
| `silver_cestujici` | `person_sk` | 61 964 |
| `silver_claim_reklamace_pokuty` | `claim_sk` | 450 |
| `silver_collection_vymahani` | `collection_action_sk` | 976 |
| `silver_platba` | `payment_sk` | 4 582 |
| `silver_podnet_cestujiciho` | `submission_sk` | 809 |
| `silver_pohledavka` | `receivable_sk` | 5 000 |
| `silver_prepravni_kontrola` | `inspection_case_sk` | 5 000 |
| `silver_refund` | `refund_sk` | 147 |
| `silver_revizor` | `inspector_sk` | 300 |

Žádný řádek nebyl vyřazen pro chybějící klíč ani duplicitu – počty v Silver odpovídají Bronze.
