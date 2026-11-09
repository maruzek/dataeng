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

## Silver – čištění a kvalita dat

Silver vzniká v notebooku `nb_silver` z každé Bronze tabulky (`bronze_x` → `silver_x`).
Tabulky se při každém běhu přepisují (overwrite), opakované spuštění tedy nevytváří duplicity.

### Názvy sloupců a datové typy
- Názvy sloupců jsou převedeny na malá písmena s podtržítky; `LOADED_AT` je přejmenován na `_loaded_at`.
- Datový typ každého sloupce je explicitně určen v notebooku (`SCHEMAS`) podle skutečného schématu zdroje:
  klíče `bigint`, data `date`, časy událostí `timestamp`, částky `decimal(10,2)`,
  příznaky (`identification_provided`, `police_assistance`, `paid_on_site`, `is_current`) `boolean`.
- `incident_time` a `inspection_time` jsou v Bronze uloženy jako `1970-01-01 hh:mm`; Spark nemá typ TIME, proto jsou převedeny na text `HH:mm:ss`.
- Hodnota, kterou nelze převést na cílový typ, je nahrazena NULL a zapsána do karantény.

### DQ pravidla
| Pravidlo | Kontrola | Akce |
|---|---|---|
| `NEPLATNY_TYP` | Hodnotu nelze převést na cílový datový typ | Hodnota → NULL, záznam v karanténě |
| `PRAZDNY_TEXT` | Prázdný text (po oříznutí mezer) | Hodnota → NULL, záznam v karanténě |
| `DATUM_V_BUDOUCNOSTI` | Datum/čas pozdější než čas načtení (neplatí pro `valid_from`, `valid_to`, `due_date`, `discount_deadline`) | Hodnota → NULL, záznam v karanténě |
| `NEREALNE_DATUM_NAROZENI` | `person_birthdate` před 1910-01-01 | Hodnota → NULL, záznam v karanténě |
| `ZAPORNA_CASTKA` | Záporná částka | Hodnota → NULL, záznam v karanténě |
| `NEPLATNE_ZNAKY_VE_JMENU` | Jméno začíná nepísmenným znakem (např. „- TETZNER“) | Znaky odstraněny, záznam v karanténě |
| `CHYBI_KLIC` | Chybí klíč tabulky | Řádek vyřazen ze Silver, uložen v karanténě |
| `DUPLICITA_KLICE` | Více řádků se stejným klíčem | Ponechán nejnovější podle `created_at`, ostatní vyřazeny do karantény |

### Karanténa – `silver_karantena`
Každá úprava nebo vyřazení záznamu je evidováno, nic nemizí bez stopy.

| Sloupec | Význam |
|---|---|
| `source_table` | Bronze tabulka, ze které záznam pochází |
| `record_key` | Klíč záznamu |
| `column_name` | Dotčený sloupec (u vyřazených řádků prázdné) |
| `original_value` | Původní hodnota z Bronze |
| `dq_rule` | Porušené pravidlo |
| `action` | `NAHRAZENO_NULL`, `UPRAVENO` nebo `VYRAZEN_RADEK` |
| `_loaded_at` | Čas načtení záznamu do Bronze |
| `_record_json` | Celý původní řádek z Bronze (dohledatelný původ) |
| `_quarantined_at` | Čas zápisu do karantény |

### Výsledek posledního běhu

| Tabulka | Řádků v Silver |
|---|---:|
| `silver_cestujici` | 61 964 |
| `silver_claim_reklamace_pokuty` | 450 |
| `silver_collection_vymahani` | 976 |
| `silver_platba` | 4 582 |
| `silver_podnet_cestujiciho` | 809 |
| `silver_pohledavka` | 5 000 |
| `silver_prepravni_kontrola` | 5 000 |
| `silver_refund` | 147 |
| `silver_revizor` | 300 |

Žádný řádek nebyl vyřazen (bez chybějících klíčů a duplicit). Zachycené vady v karanténě:

| Tabulka | Sloupec | Pravidlo | Akce | Počet |
|---|---|---|---|---:|
| `bronze_cestujici` | `person_birthdate` | `NEREALNE_DATUM_NAROZENI` | `NAHRAZENO_NULL` | 656 |
| `bronze_cestujici` | `person_firstname` | `NEPLATNE_ZNAKY_VE_JMENU` | `UPRAVENO` | 1 |
| `bronze_cestujici` | `person_full_name` | `NEPLATNE_ZNAKY_VE_JMENU` | `UPRAVENO` | 2 |
| `bronze_cestujici` | `person_surname` | `NEPLATNE_ZNAKY_VE_JMENU` | `UPRAVENO` | 2 |
| `bronze_claim_reklamace_pokuty` | `resolved_at` | `DATUM_V_BUDOUCNOSTI` | `NAHRAZENO_NULL` | 4 |
| `bronze_platba` | `created_at` | `DATUM_V_BUDOUCNOSTI` | `NAHRAZENO_NULL` | 2 |
| `bronze_platba` | `payment_timestamp` | `DATUM_V_BUDOUCNOSTI` | `NAHRAZENO_NULL` | 2 |
| `bronze_podnet_cestujiciho` | `resolved_at` | `DATUM_V_BUDOUCNOSTI` | `NAHRAZENO_NULL` | 17 |
