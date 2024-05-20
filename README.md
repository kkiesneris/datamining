# datamining
## Datizraces metožu izmantošana krāpniecisku darījumu atklāšanai
## Applying data mining techniques in fraud detection

Zemāk uzskaitītas repozitorijā atrodamās programmas un skripti aptuvenā to izmantošanas secībā, īsi aprakstot kādam mērķim tie tika lietoti. Tie nav rakstīti, lai bez izmaiņām būtu iespējams atkārtot visas darbā veiktās darbības. Atsevišķi skripti un SQL vaicājumi netika saglabāti. Tāpat atsevišķi skripti tika vairākkārt modificēti, lai atsevišķos pētījuma posmos veiktu nepieciešamos eksperimentus – tie bija tikai palīginstruments pētījuma veikšanā.

* Transakciju datu ielasīšana no datnēm tika veikta izmantojot programmas, kuru pirmkods atrodas:
`LoadATMData.cpp`
`LoadPOSData.cpp`
Šīs programmas izmanto XML konfigurāciju, kurā norādīti datnēs pieejamie lauki, to garumi un cita nepieciešamā informācija. Tā kā konfigurācija satur konfidenciālu datu struktūru, tad tā publiskai piekļuvei netiks izvietota tā pat kā tabulu struktūra, kurā šie dati tika saglabāti.

* Krāpniecisko datu tabulu struktūra tika izveidota ar SQL skriptu:
`scripts/create_fraud_data_tables.sql`
* Krāpnieciskie dati tika ielasīti no Excel datnes izmantojot python skriptu:
`scripts/extract_xls_data.py`
* Nepieciešamo datu atlasei no tabulām izveidoti skati ar SQL skriptu:
`scripts/create_views.sql`
* Krāpniecisko datu meklēšanai transakciju datos tika izmantoti python skripti:
`scripts/match_*.py`
* Kaimiņvalstu dati tika ielasīti tabulā, kas tika izveidota ar SQL skriptu:
`scripts/create_country_borders.sql`
* Kaimiņvalstu dati tika ielasīti no CSV datnes, izmantojot python skriptu:
`scripts/import_country_borders.py`
* Maksājumu karšu numuri datu transformāciju veikšanai un transakciju atlasei tika saglabāti tabulā, kura tika izveidota izmantojot SQL skriptu:
`scripts/create_pan_list.sql`
* Maksājumu karšu numuri šajā tabulā tika ierakstīti izmantojot SQL skriptu:
`scripts/insert_pan_list.sql`
* Lai būtu ērtāk atlasīt noteiktu datu kopu transakcijas, tika izveidota tabula, kurā glabājās kartes numurs un datu kopas identifikators:
`scripts/create_pan_set.sql`
* Dati par krāpnieciskajām transakcijām šajā tabulā tika ievietoti izmantojot SQL vaicājumu, bet, datu kopām, kuras saturēja pēc nejaušības principa atlasītu maksājumu karšu datus noteiktā apjomā tika izmantots python skripts:
`scripts/insert_random_pans.py`
* Transformēto datu glabāšanai tika izveidotas tabulas ar SQL skriptiem:
`scripts/create_transactions.sql`
`scripts/create_transactions_2.sql`
* Datu transformācijai tika izmantoti python skripti:
`scripts/transform_data.py`
`scripts/transform_data_2.py`
* Datu atlasei priekš datu kopām tika izmantots skats, kas tika izveidots ar SQL skriptu:
`scripts/create_transaction_vw.sql`

Datu analīze, diagrammu ģenerēšana, atribūtu atlase, dimensiju redukcija, un klasifikācijas algoritmu apmācīšana un testēšana tika veikta ar python skriptiem izmantojot Jupyter Notebook rīku.
* Datu analīzei tika izmantots:
`jupyter/data_analysis.ipynb`
* Korelāciju matricas ģenerēšanai:
`jupyter/correlation_matrix.ipynb`
* Atribūtu atlasei:
`jupyter/ReliefF.ipynb`
`jupyter/CART.ipynb`
* Dimensiju redukcijai
`jupyter/PCA.ipynb`
`jupyter/tSNE.ipynb`
`jupyter/UMAP.ipynb`
* Mapper grafa ģenerēšanai:
`jupyter/mapper_tSNE_UMAP.ipynb`
* Klasifikācijai:
`jupyter/classification.ipynb`
`jupyter/classification_test.ipynb`
* Atribūtu kopu saraksts un dažas koplietošanas funkcijas glabājas:
`jupyter/dm_lib.py`
