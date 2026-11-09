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
