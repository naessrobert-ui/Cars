import os
import re
import pandas as pd
import json
from datetime import datetime, date

# ===================== Konfig =====================
DATA_KATALOG = r'c:\Users\Rober\Finn_Bil'
OUTPUT_PARQUET_FIL = 'database_biler.parquet'
OUTPUT_METADATA_FIL = 'metadata.json'

FN_PATTERN = r'(\d{2}-\d{2}-\d{4})'


def parse_dato(filnavn: str):
    m = re.search (FN_PATTERN, filnavn)
    if not m: return None
    try:
        return datetime.strptime (m.group (1), "%d-%m-%Y").date ()
    except ValueError:
        return None


def konsolider_csv_til_parquet(min_dato_filter: date | None):
    alle_filer = []
    print ("Søker...");
    for f in os.listdir (DATA_KATALOG):
        if f.startswith ('biler_alle_') and f.endswith ('.csv'):
            dato = parse_dato (f)
            if dato and (min_dato_filter is None or dato >= min_dato_filter):
                alle_filer.append ((dato, os.path.join (DATA_KATALOG, f)))
    alle_filer.sort (key=lambda x: x[0])
    print (f"Fant {len (alle_filer)} filer.")

    alle_dfs = []
    for dato, filsti in alle_filer:
        try:
            print (f"Leser fil for dato {dato:%d-%m-%Y}...")
            df = pd.read_csv (filsti, sep=';', encoding='utf-16', on_bad_lines='warn')
            df.columns = [str (c) for c in df.columns]

            # Direkte og presis omdøping
            df.rename (columns={
                'Modell': 'Overskrift',
                'Info': 'Modell',
                'Rekkevidde': 'rekkevidde_str'  # Standardiserer til små bokstaver
            }, inplace=True)

            # Sikre at kolonner alltid finnes
            for col in ['Overskrift', 'Modell', 'Produsent', 'rekkevidde_str', 'hjuldrift', 'drivstoff']:
                if col not in df.columns: df[col] = pd.NA

            df['Produsent'] = df['Bilmerke'].astype (str).apply (lambda x: x.split (' ', 1)[0].strip ())

            df.loc[
                df['drivstoff'].astype (str).str.contains ('km|rekkevidde', case=False, na=False), 'drivstoff'] = pd.NA

            df['FinnKode'] = df['FinnKode'].astype (str).str.replace (r'\D', '', regex=True).str.lstrip ('0')
            df = df.dropna (subset=['FinnKode'])
            df = df[df['FinnKode'] != '']
            df['Dato'] = pd.to_datetime (dato)

            if 'Pris' in df.columns:
                df['Pris_num'] = pd.to_numeric (df['Pris'], errors='coerce')
                df.loc[df['Pris'].astype (str).str.strip ().str.lower () == 'solgt', 'Pris_num'] = 0

            kolonner_a_beholde = ['FinnKode', 'Dato', 'Produsent', 'Modell', 'Overskrift', 'årstall', 'kjørelengde',
                                  'drivstoff', 'hjuldrift', 'rekkevidde_str', 'selger', 'Pris_num']
            alle_dfs.append (df[[k for k in kolonner_a_beholde if k in df.columns]])
        except Exception as e:
            print (f"  FEIL ved lesing av {os.path.basename (filsti)}: {e}")

    if not alle_dfs: print ("Ingen data lest."); return
    master_df = pd.concat (alle_dfs, ignore_index=True)

    print ("Renser og konverterer datatyper til heltall...")
    for col in ['årstall', 'kjørelengde', 'Pris_num', 'rekkevidde_str']:
        if col in master_df.columns:
            master_df[col] = pd.to_numeric (master_df[col], errors='coerce').fillna (0).astype ('int64')

    master_df.to_parquet (OUTPUT_PARQUET_FIL, index=False)
    print (f"Lagret til Parquet-fil: {OUTPUT_PARQUET_FIL}")

    # ... (Resten er uendret, men vil nå finne korrekte kolonner)
    produsenter = sorted (master_df['Produsent'].dropna ().unique ().tolist ())
    models_by_prod = master_df.dropna (subset=['Produsent', 'Modell']).groupby ('Produsent')['Modell'].apply (
        lambda x: sorted (list (x.unique ()))).to_dict ()
    drivstoff_opts = sorted (
        master_df['drivstoff'].dropna ().unique ().tolist ()) if 'drivstoff' in master_df.columns else []
    hjuldrift_opts = sorted (
        master_df['hjuldrift'].dropna ().unique ().tolist ()) if 'hjuldrift' in master_df.columns else []
    metadata = {'produsenter': produsenter, 'models_by_prod': models_by_prod, 'drivstoff_opts': drivstoff_opts,
                'hjuldrift_opts': hjuldrift_opts, 'year_min': int (master_df['årstall'].min ()),
                'year_max': int (master_df['årstall'].max ()), 'km_min': int (master_df['kjørelengde'].min ()),
                'km_max': int (master_df['kjørelengde'].max ()),
                'latest_dt': master_df['Dato'].max ().strftime ('%Y-%m-%d')}
    with open (OUTPUT_METADATA_FIL, 'w', encoding='utf-8') as f:
        json.dump (metadata, f, indent=4)
    print (f"Lagret metadata til: {OUTPUT_METADATA_FIL}")
    print ("\nFerdig!")


if __name__ == "__main__":
    min_dato = None
    dato_input = input ("Angi en startdato (ÅÅÅÅ-MM-DD) eller trykk Enter: ")
    if dato_input.strip ():
        try:
            min_dato = datetime.strptime (dato_input.strip (), "%Y-%m-%d").date ()
        except ValueError:
            print ("Ugyldig datoformat."); exit ()
    konsolider_csv_til_parquet (min_dato_filter=min_dato)