import pandas as pd
import psycopg2
from pathlib import Path
from zipfile import ZipFile
from os import unlink


def extrai(zipf):
    with ZipFile(zipf) as zp:
        zp.extractall("entradas/")
    # unlink(zipf)


def at_bd(prevs):
    con = psycopg2.connect(
        "dbname='ENERGIA' host='192.168.0.251' user='script' password='batata'")
    cur = con.cursor()

    with open("saídas/ena.csv", "r") as fp:
        if prevs:
            header = fp.readline()
            cur.copy_from(fp, 'pluvia_enaprevs', ";")
        else:
            header = fp.readline()
            cur.copy_from(fp, 'pluvia_ena', ";")

    con.commit()
    con.close()


def trata_enaprevs(zipf, loc_csv):

    df = pd.read_csv(loc_csv, delimiter=";", na_values="M")
    # df.set_index("PREVS", inplace=True)

    df["ENA_Percentual_MLT"] = df["ENA_Percentual_MLT"].str.replace(
        ",", ".")
    df["ENA_Percentual_MLT"] = df["ENA_Percentual_MLT"].astype(float)
    df["ENA_MWmes"] = df["ENA_MWmes"].str.replace(",", ".")
    df["ENA_MWmes"] = df["ENA_MWmes"].astype(float)

    df["zip"] = zipf.stem.replace("-ENA", "")
    df["csv"] = loc_csv.stem

    df = df.fillna(9)
    df["Semana"] = df["Semana"].astype(int)

    return df


def trata_ena(zipf, loc_csv):
    df = pd.read_csv(loc_csv, delimiter=";", skiprows=59, na_values="MEDIA")
    # df.set_index("Tipo", inplace=True)

    df["ENA_Percentual_MLT"] = df["ENA_Percentual_MLT"].str.replace(
        ",", ".")
    df["ENA_Percentual_MLT"] = df["ENA_Percentual_MLT"].astype(float)
    df["ENA_MWmes"] = df["ENA_MWmes"].str.replace(",", ".")
    df["ENA_MWmes"] = df["ENA_MWmes"].astype(float)
    df["zip"] = zipf.stem.replace("-ENA", "")
    df["csv"] = loc_csv.stem

    df = df.fillna("1979-01-01")

    return df


def main():
    locs = Path("entradas/").glob("**/*")
    zips = [loc for loc in locs if loc.is_file() and loc.suffix == ".zip"]

    for zipf in zips:
        extrai(zipf)
        locs = Path("entradas/").glob("**/*")
        csvs = [loc for loc in locs if loc.is_file() and loc.suffix == ".csv"]

        for csv in csvs:
            prevs = "ENAPREVS" in csv.stem
            if prevs:
                df = trata_enaprevs(zipf, csv)
            else:
                df = trata_ena(zipf, csv)

            df.to_csv("saídas/ena.csv", ";")
            # print("Adicionando %s ao banco de dados" % csv.stem)
            # at_bd(prevs)


main()
