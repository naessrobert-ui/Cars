import os
import re
import json
from datetime import datetime, date
import io

import pandas as pd
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import awswrangler as wr
import boto3
from dotenv import load_dotenv

# ===================== Konfig =====================
BASE_DIR = os.path.dirname (os.path.abspath (__file__))
DOTENV_PATH = os.path.join (BASE_DIR, '.env')
if os.path.exists (DOTENV_PATH):
    print (f"Laster .env fil fra: {DOTENV_PATH}")
    load_dotenv (dotenv_path=DOTENV_PATH)

LOCAL_METADATA_FIL = os.path.join (BASE_DIR, 'metadata.json')
AWS_S3_BUCKET_NAME = os.environ.get ("AWS_S3_BUCKET_NAME")
AWS_S3_REGION = os.environ.get ("AWS_S3_REGION")
ATHENA_DATABASE = os.environ.get ("ATHENA_DATABASE", "default")
DEFAULT_STARTDATE = date (2025, 7, 1)

if AWS_S3_REGION:
    boto3.setup_default_session (region_name=AWS_S3_REGION)

# ===================== App-initialisering =====================
app = dash.Dash (__name__, external_stylesheets=[dbc.themes.FLATLY])
server = app.server


# ===================== Hjelpere =====================
def _to_numeric_from_text(s: pd.Series):
    return pd.to_numeric (s.astype (str).str.extract (r'(\d+)', expand=False), errors='coerce')


# ===================== Metadata-innlesing =====================
prod_list, models_by_prod_initial, drivstoff_pref_opts, hjuldrift_pref_opts = [], {}, [], []
year_min, year_max, km_min, km_max, latest_dt = 2000, date.today ().year, 0, 300000, date.today ()
try:
    if AWS_S3_BUCKET_NAME and AWS_S3_REGION:
        s3 = boto3.client ('s3')
        meta_obj = s3.get_object (Bucket=AWS_S3_BUCKET_NAME, Key='metadata.json')
        metadata = json.loads (meta_obj['Body'].read ().decode ('utf-8'))
    else:
        with open (LOCAL_METADATA_FIL, 'r', encoding='utf-8') as f:
            metadata = json.load (f)
    prod_list = metadata.get ('produsenter', []);
    models_by_prod_initial = metadata.get ('models_by_prod', {});
    drivstoff_pref_opts = metadata.get ('drivstoff_opts', []);
    hjuldrift_pref_opts = metadata.get ('hjuldrift_opts', []);
    year_min = metadata.get ('year_min', 2000);
    year_max = metadata.get ('year_max', date.today ().year);
    km_min = metadata.get ('km_min', 0);
    km_max = metadata.get ('km_max', 300000);
    latest_dt = date.fromisoformat (metadata.get ('latest_dt', date.today ().isoformat ()))
except Exception as e:
    print (f"ADVARSEL: Kunne ikke laste metadata. Feil: {e}")


# ===================== Hovedlogikk med Athena (ENDELIG FIKS) =====================
def load_and_process_data(selected_produsent, selected_modell, min_dato, max_dato, **kwargs):
    where_clauses = [f"produsent = '{selected_produsent}'", f"date(dato) >= DATE('{min_dato.isoformat ()}')",
                     f"date(dato) <= DATE('{max_dato.isoformat ()}')"]
    if selected_modell:
        safe_modell = selected_modell.replace ("'", "''")
        where_clauses.append (f"modell = '{safe_modell}'")
    query = f"SELECT * FROM biler WHERE {' AND '.join (where_clauses)}"
    print (f"Kjører Athena-spørring: {query}")
    try:
        df = wr.athena.read_sql_query (sql=query, database=ATHENA_DATABASE,
                                       s3_output=f"s3://{AWS_S3_BUCKET_NAME}/athena-results/")
    except Exception as e:
        print (f"Feil under kjøring av Athena-spørring: {e}");
        return pd.DataFrame (), []
    if df.empty: return pd.DataFrame (), []
    df.columns = [c.lower () for c in df.columns]
    if kwargs.get ('pf_drivstoff'): df = df[df['drivstoff'].isin (kwargs['pf_drivstoff'])]
    if kwargs.get ('pf_hjuldrift'): df = df[df['hjuldrift'].isin (kwargs['pf_hjuldrift'])]
    if kwargs.get ('pf_year_min') is not None: df = df[df['årstall'] >= kwargs['pf_year_min']]
    if kwargs.get ('pf_year_max') is not None: df = df[df['årstall'] <= kwargs['pf_year_max']]
    df_filtered = df.copy ()
    if df_filtered.empty: return pd.DataFrame (), []

    # Aggregert statistikk
    daily_stats_df = df_filtered.groupby ('dato').agg (Antall_Totalt=('finnkode', 'size'),
                                                       Antall_Solgt=('pris_num', lambda x: (x == 0).sum ()),
                                                       Median_Pris_Usolgt=(
                                                       'pris_num', lambda x: x[x > 0].median ())).reset_index ()
    daily_stats_df['Median_Pris_Usolgt'] = daily_stats_df['Median_Pris_Usolgt'].astype (object).where (
        pd.notna (daily_stats_df['Median_Pris_Usolgt']), None)

    # === KRITISK FIKS 1: Konverter 'dato' til streng og gi den nytt navn ===
    daily_stats_df['Dato'] = pd.to_datetime (daily_stats_df['dato']).dt.strftime ('%Y-%m-%d')
    daily_stats_df = daily_stats_df.drop (columns=['dato'])  # Fjern den gamle Timestamp-kolonnen
    daily_stats = daily_stats_df.to_dict ('records')

    # Bygg historikk
    historikk = df_filtered.sort_values ('dato').groupby ('finnkode').agg (Produsent=('produsent', 'last'),
                                                                           Modell=('modell', 'last'),
                                                                           Overskrift=('overskrift', 'last'),
                                                                           årstall=('årstall', 'last'),
                                                                           kjørelengde=('kjørelengde', 'last'),
                                                                           drivstoff=('drivstoff', 'last'),
                                                                           hjuldrift=('hjuldrift', 'last'),
                                                                           Rekkevidde_str=('rekkevidde_str', 'last'),
                                                                           selger=('selger', 'last'),
                                                                           Dato_start=('dato', 'first'),
                                                                           Dato_end=('dato', 'last'),
                                                                           Pris_start=('pris_num', 'first'), Pris_last=(
        'pris_num', 'last')).reset_index ()
    historikk.columns = [c.lower () for c in historikk.columns]
    historikk['dager'] = (pd.to_datetime (historikk['dato_end']) - pd.to_datetime (historikk['dato_start'])).dt.days
    historikk['prisfall'] = historikk['pris_last'] - historikk['pris_start']
    historikk['rekkevidde'] = historikk['rekkevidde_str']

    # === KRITISK FIKS 2: Konverter alle dato-kolonner til strenger FØR retur ===
    historikk['dato_start'] = pd.to_datetime (historikk['dato_start']).dt.strftime ('%Y-%m-%d')
    historikk['dato_end'] = pd.to_datetime (historikk['dato_end']).dt.strftime ('%Y-%m-%d')

    return historikk, daily_stats


# ===================== Health Check =====================
@server.route ('/health')
def health_check(): return "OK", 200


# ===================== Layout =====================
app.layout = dbc.Container ([
    dcc.Store (id='stored-cars-data'), dcc.Store (id='stored-daily-stats'),
    dcc.Store (id='models-by-prod-store', data=models_by_prod_initial),
    dbc.Row (dbc.Col (html.H1 ("Analyse av bruktbilmarkedet", className="text-center my-4"))),
    dbc.Row (dbc.Col (dbc.Alert ([html.H4 ("Velkommen!"), dcc.Markdown (
        "1. **Velg merke/modell** og startdato.\n2. Bruk **forhåndsfiltrene** for å avgrense datasettet.\n3. Klikk **\"Last inn data\"**.\n4. Utforsk med **visningsfiltrene**.")],
                                 color="info"))),
    dbc.Row (dbc.Col (html.H4 ("1. Velg data og forhåndsfiltrer"), className="mt-4")),
    dbc.Row ([dbc.Col ([html.Label ("Produsent"), dcc.Dropdown (id='dropdown-produsent', options=prod_list)], md=2),
              dbc.Col ([html.Label ("Modell"), dcc.Dropdown (id='dropdown-modell', disabled=True)], md=2), dbc.Col (
            [html.Label ("Startdato"),
             dcc.DatePickerSingle (id='input-startdato', date=DEFAULT_STARTDATE, display_format='DD-MM-YYYY',
                                   clearable=True, style={'width': '100%'})], md=2), dbc.Col (
            [html.Label ("Drivstoff"), dcc.Dropdown (id='prefilt-drivstoff', options=drivstoff_pref_opts, multi=True)],
            md=2), dbc.Col (
            [html.Label ("Hjuldrift"), dcc.Dropdown (id='prefilt-hjuldrift', options=hjuldrift_pref_opts, multi=True)],
            md=2)], className="mb-3 g-2"),
    dbc.Row ([dbc.Col ([html.Label ("Årstall fra/til"), dbc.Row (
        [dbc.Col (dcc.Input (id='prefilt-year-fra', type='number', placeholder=year_min)),
         dbc.Col (dcc.Input (id='prefilt-year-til', type='number', placeholder=year_max))])], md=3), dbc.Col (
        [html.Label ("Kjørelengde fra/til"), dbc.Row (
            [dbc.Col (dcc.Input (id='prefilt-km-fra', type='number', placeholder=km_min)),
             dbc.Col (dcc.Input (id='prefilt-km-til', type='number', placeholder=km_max))])], md=3)],
             className="mb-3 g-2"),
    dbc.Row (
        dbc.Col (dbc.Button ('Last inn data', id='load-data-button', color="primary", size="lg", className="w-100"),
                 md=4), className="mb-3"),
    dbc.Row (dbc.Col (html.Div (id='loading-output', className="text-center text-muted"))),
    dbc.Row (dbc.Col (html.Hr (), className="my-4")),
    dbc.Row (dbc.Col (html.H2 ("Aggregert statistikk", className="text-center mb-3"))),
    dbc.Spinner (html.Div (id='daily-stats-output')),
    dbc.Row (dbc.Col (html.Hr (), className="my-4")),
    dbc.Row (dbc.Col (html.H2 ("2. Utforsk resultater", className="text-center mb-3"))),
    dbc.Card ([dbc.CardHeader ("Visningsfiltre"), dbc.CardBody ([
        dbc.Row ([dbc.Col ([html.Label ("Drivstoff"), dcc.Dropdown (id='res-drivstoff', multi=True)], md=3),
                  dbc.Col ([html.Label ("Hjuldrift"), dcc.Dropdown (id='res-hjuldrift', multi=True)], md=3),
                  dbc.Col ([html.Label ("Søk i annonseoverskrift"), dcc.Input (id='res-modell-sok', debounce=True)],
                           md=3),
                  dbc.Col ([html.Label ("Selger"), dcc.Input (id='res-seller-sok', debounce=True)], md=3)],
                 className="mb-3 g-2"),
        dbc.Row ([dbc.Col ([html.Label ("Årstall fra/til"), dbc.Row (
            [dbc.Col (dbc.Input (id='res-year-fra', type='number', placeholder=year_min)),
             dbc.Col (dbc.Input (id='res-year-til', type='number', placeholder=year_max))])], md=3), dbc.Col (
            [html.Label ("Pris"),
             dcc.RangeSlider (id='res-price-slider', min=0, max=2000000, step=10000, value=[20000, 2000000], marks=None,
                              tooltip={"placement": "bottom", "always_visible": True})], md=3), dbc.Col (
            [html.Label ("Kjørelengde"),
             dcc.RangeSlider (id='res-km-slider', min=0, max=300000, step=5000, value=[0, 300000], marks=None,
                              tooltip={"placement": "bottom", "always_visible": True})], md=3), dbc.Col (
            [html.Label ("Dager til salgs"),
             dcc.RangeSlider (id='res-days-slider', min=0, max=365, step=5, value=[0, 365], marks=None,
                              tooltip={"placement": "bottom", "always_visible": True})], md=3)], className="g-2")
    ])], className="mb-4"),
    dbc.Row (dbc.Col (html.Div (id='antall-biler-funnet', className="text-center fw-bold fs-5 mb-3"))),
    dbc.Spinner (html.Div (id='output-bil-tabell'))
], fluid=True, className="dbc")


# ===================== Callbacks =====================
@app.callback (Output ('dropdown-modell', 'options'), Output ('dropdown-modell', 'disabled'),
               Input ('dropdown-produsent', 'value'), State ('models-by-prod-store', 'data'))
def set_modell_options(selected_produsent, models_by_prod_data):
    if not selected_produsent: return [], True
    return models_by_prod_data.get (selected_produsent, []), False


@app.callback (Output ('stored-cars-data', 'data'), Output ('stored-daily-stats', 'data'),
               Output ('loading-output', 'children'), Output ('res-drivstoff', 'options'),
               Output ('res-hjuldrift', 'options'), Input ('load-data-button', 'n_clicks'),
               State ('dropdown-produsent', 'value'), State ('dropdown-modell', 'value'),
               State ('input-startdato', 'date'), State ('prefilt-drivstoff', 'value'),
               State ('prefilt-hjuldrift', 'value'), State ('prefilt-km-fra', 'value'),
               State ('prefilt-km-til', 'value'), State ('prefilt-year-fra', 'value'),
               State ('prefilt-year-til', 'value'))
def load_selected_data(n_clicks, produsent, modell, startdato_str, pf_drivstoff, pf_hjuldrift, km_fra, km_til, year_fra,
                       year_til):
    if not n_clicks or not produsent: raise PreventUpdate
    min_dato = date.fromisoformat (startdato_str) if startdato_str else DEFAULT_STARTDATE
    pf_year_min = int (year_fra) if year_fra is not None else None
    pf_year_max = int (year_til) if year_til is not None else None
    df_loaded, daily_stats = load_and_process_data (produsent, modell, min_dato, date.today (),
                                                    pf_drivstoff=pf_drivstoff or [], pf_hjuldrift=pf_hjuldrift or [],
                                                    pf_km_min=km_fra, pf_km_max=km_til, pf_year_min=pf_year_min,
                                                    pf_year_max=pf_year_max)
    res_drivstoff_opts = sorted (
        df_loaded['drivstoff'].dropna ().unique ()) if 'drivstoff' in df_loaded and not df_loaded.empty else []
    res_hjuldrift_opts = sorted (
        df_loaded['hjuldrift'].dropna ().unique ()) if 'hjuldrift' in df_loaded and not df_loaded.empty else []
    msg = f"Lastet inn {len (df_loaded)} bilhistorikker."
    return (df_loaded.to_json (orient='split'), json.dumps (daily_stats), msg, res_drivstoff_opts, res_hjuldrift_opts)


@app.callback (Output ('daily-stats-output', 'children'), Input ('stored-daily-stats', 'data'))
def display_daily_stats(daily_stats_json):
    if not daily_stats_json: return dbc.Alert ("Last inn data for å se statistikk.", color="primary",
                                               className="text-center w-75 mx-auto")
    df_stats = pd.DataFrame (json.loads (daily_stats_json))
    if df_stats.empty: return dbc.Alert ("Ingen statistikk funnet.", color="warning",
                                         className="text-center w-75 mx-auto")
    df_stats['Dato'] = pd.to_datetime (df_stats['Dato']).dt.strftime ('%d-%m-%Y')
    if 'median_pris_usolgt' in df_stats.columns:
        df_stats.rename (columns={'median_pris_usolgt': 'Median_Pris_Usolgt'}, inplace=True)
        df_stats['Median_Pris_Usolgt'] = df_stats['Median_Pris_Usolgt'].apply (
            lambda x: f"{int (x):,}".replace (",", " ") if pd.notna (x) else "N/A")
    return dbc.Table.from_dataframe (df_stats, striped=True, bordered=True, hover=True, className="w-75 mx-auto")


@app.callback (
    Output ('output-bil-tabell', 'children'),
    Output ('antall-biler-funnet', 'children'),
    [Input ('stored-cars-data', 'data'), Input ('res-drivstoff', 'value'), Input ('res-hjuldrift', 'value'),
     Input ('res-modell-sok', 'value'), Input ('res-seller-sok', 'value'), Input ('res-year-fra', 'value'),
     Input ('res-year-til', 'value'), Input ('res-price-slider', 'value'), Input ('res-km-slider', 'value'),
     Input ('res-days-slider', 'value')]
)
def update_table(stored_json, f_drivstoff, f_hjuldrift, modell_sok, seller_sok, year_fra, year_til, price_range,
                 km_range, days_range):
    if not stored_json: return html.P (
        "Velg produsent/modell og trykk «Last inn data» for å begynne."), "Antall biler i utvalget: 0"
    df = pd.read_json (stored_json, orient='split')
    if df.empty: return html.P ("Ingen biler lastet inn."), "Antall biler i utvalget: 0"
    if modell_sok: df = df[df['overskrift'].astype (str).str.contains (modell_sok, case=False, na=False)]
    if f_drivstoff: df = df[df['drivstoff'].isin (f_drivstoff)]
    if f_hjuldrift: df = df[df['hjuldrift'].isin (f_hjuldrift)]
    if seller_sok: df = df[df['selger'].astype (str).str.contains (seller_sok, case=False, na=False)]
    if year_fra is not None: df = df[df['årstall'] >= year_fra]
    if year_til is not None: df = df[df['årstall'] <= year_til]
    df = df[df['pris_last'].between (price_range[0], price_range[1])]
    df = df[df['kjørelengde'].between (km_range[0], km_range[1])]
    df = df[df['dager'].between (days_range[0], days_range[1])]
    ant_txt = f"Antall biler i utvalget: {len (df)}"
    if df.empty: return dbc.Alert ("Ingen biler passer til de valgte visningsfiltrene.", color="warning"), ant_txt
    f_display = df.copy ()
    if 'årstall' in f_display.columns: f_display['årstall'] = f_display['årstall'].astype ('Int64')
    for col in ['dato_start', 'dato_end']:
        if col in f_display.columns: f_display[col] = pd.to_datetime (f_display[col]).dt.strftime ('%d.%m.%y')
    base_url = "https://www.finn.no/car/used/ad.html?finnkode="
    f_display['finnkode'] = f_display['finnkode'].apply (lambda fk: f'[{fk}]({base_url}{fk})')
    display_cols = ['finnkode', 'overskrift', 'årstall', 'kjørelengde', 'rekkevidde', 'pris_last', 'prisfall', 'dager',
                    'drivstoff', 'hjuldrift', 'selger', 'dato_start', 'dato_end']
    table_columns = []
    for col in display_cols:
        name = "Annonseoverskrift" if col == "overskrift" else col.replace ("_", " ").title ()
        if col == 'finnkode':
            table_columns.append ({"name": "FINN-kode", "id": col, "presentation": "markdown"})
        else:
            table_columns.append ({"name": name, "id": col})
    tbl = dash_table.DataTable (id='table', columns=table_columns, data=f_display.to_dict ('records'),
                                markdown_options={'link_target': '_blank'}, sort_action="native", page_size=15,
                                style_cell={'textAlign': 'left', 'padding': '5px', 'whiteSpace': 'normal',
                                            'height': 'auto'}, style_header={'fontWeight': 'bold'},
                                style_table={'overflowX': 'auto'})
    return tbl, ant_txt


if __name__ == "__main__":
    app.run (debug=True, port=8050)