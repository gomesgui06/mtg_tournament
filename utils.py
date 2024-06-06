import fitz
import pandas as pd
import numpy as np
import re
import os
import shutil
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials

class PDFExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.text = self.extract_text_from_first_page()

    def extract_text_from_first_page(self):
        document = fitz.open(self.pdf_path)
        first_page = document[0]
        text = first_page.get_text()
        return text

    def extract_round(self):
        match = re.search(r"Rodada (\d+)", self.text)
        if match:
            valor_rodada = match.group(1)
            return valor_rodada
        else:
            print("Rodada não encontrada no texto.")
            return None

    def extract_date(self):
        match = re.search(r"Data do evento:\s*(.*)", self.text)
        if match:
            data = match.group(1)
            return data
        else:
            print("Data não encontrada no texto.")
            return None

    def extract_tournament_name(self):
        match = re.search(r"Evento:\s*(.*)", self.text)
        if match:
            evento = match.group(1)
            return evento
        else:
            print("Evento não encontrado no texto.")
            return None

    def process_text_rodada(self):
        pattern = re.compile(r"(\d+)\s+(.+?)\s{2,}(.+?)\s{2,}(\d+-\d+)")
        matches = pattern.findall(self.text)
        rodada = self.extract_round()
        date = self.extract_date()
        tournament_name = self.extract_tournament_name()

        data = []
        for match in matches:
            mesa, jogador, oponente, pontos = match
            data.append({
                "rodada": rodada,
                "mesa": mesa,
                "jogador": jogador,
                "oponente": oponente,
                "pontos": pontos,
                "data": date,
                "nome_torneio": tournament_name
            })
        return data

    def process_text_final(self):
        pattern = re.compile(r'^\s*\d+\s+(.+?)(\s+\d+)', re.MULTILINE)
        matches = pattern.findall(self.text)
        date = self.extract_date()  # Corrigir a chamada para self.extract_date
        data = []
        for match in matches:
            jogador = match[0].strip()
            pontos = match[1].strip()
            data.append({
                "jogadores": jogador,
                "data": date,
                "pontuacao": pontos,
                
            })
        return data        

    @staticmethod
    def list_files_in_folder(folder_path):
        return os.listdir(folder_path)

    @staticmethod
    def move_files(source_folder, destination_folder):
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        for file_name in os.listdir(source_folder):
            source_file_path = os.path.join(source_folder, file_name)
            destination_file_path = os.path.join(destination_folder, file_name)
            shutil.move(source_file_path, destination_file_path)
            print(f"Moved {file_name} to {destination_folder}")

    @staticmethod
    def process_data_rodada(path, input_folder, output_folder, date):
        pdf_folder_path = f'{path}/{input_folder}/'
        files = PDFExtractor.list_files_in_folder(pdf_folder_path)
        for file in files:
            pdf_path = f"{pdf_folder_path}/{file}"
            extractor = PDFExtractor(pdf_path)
            data = extractor.process_text_rodada()
            df = pd.DataFrame(data)
            df[['pontos_jogador', 'pontos_oponente']] = df['pontos'].str.split('-', expand=True)
            file = file.split(".pdf")[0]
            csv_path = f'{path}/{output_folder}/emparelhamento_por_nome/resultados {file} {date}.csv'
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f'Dados processados na pasta {path}/{output_folder}')

    @staticmethod
    def process_data_final(path, input_folder, output_folder, date):
        pdf_folder_path = f'{path}/{input_folder}/'
        files = PDFExtractor.list_files_in_folder(pdf_folder_path)
        for file in files:
            pdf_path = f"{pdf_folder_path}/{file}"
            extractor = PDFExtractor(pdf_path)  # Usar a classe PDFExtractor
            data = extractor.process_text_final()  # Corrigir a chamada para process_text_final
        df = pd.DataFrame(data)
        df['deck'] = np.nan
        df['participacao_liga'] = 'sim'
        file = file.split(".pdf")[0]
        csv_path = f'{path}/{output_folder}/emparelhamento_classificacao/resultados {file} {date}.csv'    
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f'Dados processados na pasta {path}/{output_folder}') 
        return df             

    @staticmethod
    def read_dataframes_in_folder(path, output_folder):
        all_files = PDFExtractor.list_files_in_folder(f"{path}/{output_folder}/emparelhamento_por_nome/")
        li = []
        for filename in all_files:
            df = pd.read_csv(f'{path}/{output_folder}/emparelhamento_por_nome/{filename}', index_col=None, header=0)
            li.append(df)
        dataframes = pd.concat(li, axis=0, ignore_index=True)
        print(f'dataframes lidos da pasta: {all_files}')
        return dataframes

    @staticmethod
    def format_date(path):
        extractor = PDFExtractor(path)
        date = extractor.extract_date()
        date_obj = datetime.strptime(date, "%d/%m/%Y")
        new_date_str = date_obj.strftime("%Y_%m_%d")
        return new_date_str

    @staticmethod
    def format_date_normal(path):
        extractor = PDFExtractor(path)
        date = extractor.extract_date()
        date_obj = datetime.strptime(date, "%d/%m/%Y")
        new_date_str = date_obj.strftime("%d/%m/%Y")
        return str(new_date_str)


    @staticmethod
    def write_dataframe_google_sheets(path, spreadsheet_id, worksheet_id, df):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(f'{path}/torneio-magic-pauper-0f7744989ed1.json', scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.get_worksheet_by_id(worksheet_id)

        existing = get_as_dataframe(worksheet)
        existing = existing.dropna(how='all')
        new_df = pd.concat([existing, df])
        set_with_dataframe(worksheet, new_df)
        print("DataFrame written to Google Sheet successfully.")
    
    @staticmethod
    def get_dataframe_google_sheets(path, spreadsheet_id, worksheet_id):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(f'{path}/torneio-magic-pauper-0f7744989ed1.json', scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.get_worksheet_by_id(worksheet_id)

        existing = get_as_dataframe(worksheet)
        existing = existing.dropna(how='all')
        return existing


    @staticmethod
    def determinar_resultado(row):
        if row['pontos_jogador'] > row['pontos_oponente']:
            return 'vitória'
        elif row['pontos_jogador'] < row['pontos_oponente']:
            return 'derrota'
        else:
            return 'empate'
