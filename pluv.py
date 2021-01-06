import pandas as pd
import psycopg2
from pathlib import Path
from zipfile import ZipFile
from os import unlink
from datetime import  date
from gpluv import gpl


def extrai(zipf):
    with ZipFile(zipf) as zp:
        zp.extractall("entradas/")
    unlink(zipf)


def at_bd(prevs):
    con = psycopg2.connect(
        "dbname='ENERGIA' host='192.168.0.251' user='script' password='batata'")
    cur = con.cursor()

    with open("saídas/ena.csv", "r") as fp:
        try:
            if prevs:
                header = fp.readline()
                cur.copy_from(fp, 'pluvia_enaprevs', ";")
            else:
                header = fp.readline()
                cur.copy_from(fp, 'pluvia_ena', ";")
        except:
            print("Não foi possível adicionar esse arquivo ao banco de dados")
        else:
            con.commit()
        finally:
            con.close()




def trata_enaprevs(zipf, loc_csv):

    df = pd.read_csv(loc_csv, delimiter=";", na_values="M")
    # df.set_index("PREVS", inplace=True)

    df["ENA_Percentual_MLT"] = df["ENA_Percentual_MLT"].str.replace(
        ",", ".")
    df["ENA_Percentual_MLT"] = df["ENA_Percentual_MLT"].astype(float)
    df["ENA_MWmes"] = df["ENA_MWmes"].str.replace(",", ".")
    df["ENA_MWmes"] = df["ENA_MWmes"].astype(float)

    set_keys(df, loc_csv)

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

    set_keys(df, loc_csv)

    df = df.fillna("1979-01-01")

    return df


def set_keys(df, loc_csv):
    df["zip"] = date.today().strftime("%Y-%m-%d")
    df["csv"] = loc_csv.stem


def cleanup():
    arquivos = [arquivo for arquivo in Path("entradas").glob("**/*") if arquivo.is_file()]
    for arquivo in arquivos:
        print("Removendo %s" % arquivo.name)
        unlink(arquivo)

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
            print("Adicionando %s ao banco de dados" % csv.stem)
            at_bd(prevs)

    print("Limpando arquivos de entrada")
    cleanup()
    print("Concluído")


main()
