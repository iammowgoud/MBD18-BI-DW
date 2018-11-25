# Oil Production ETL using Pentaho Data Integration | BI-DW O-1-1

## Running

- Publish Schema MEXICO_PRODUCTION found in `/ERD (mwb)/Mexico Production Model.mwb` model.
- Make sure you have `production_mexico_all.json` and `wells_mexico_all.json` in the `/PDI/input` folder
- Open `JB_COMPLETE.kjb`
- Run Process
  - `1,195,489` records should be created in the Fact Table `Production_F`

## MySQL connection Details

- `jdbc:mysql://localhost:3306/`
- Username: `root`
- Password:
  - Empty string. Nothing.

## Side Effects / intermedite generated files

- The file `merged_production.csv` will appear in `/PDI/output` directory

## Output Sample

- You can find Schema populated with `845,645` records in `/MySQL Dump/MEXICO_PRODUCTION_dump.sql`
