from utils import PDFExtractor


def main():
    path = "/home/gomes/Documentos/Gomes/extrator_dados_torneio_magic"
    input_folder = "torneios_para_processar/emparelhamento_por_classificacao"
    date = PDFExtractor.format_date(f"{path}/{input_folder}/Rodada 4 Posições por classificação.pdf")  
    destination_folder = f'torneios_processados'
    output_folder = f"resultado"
    spreadsheet_id = "1zk93XeoQ6BgnEzH8NlKzakqjwXltNdODoHTtYowzFeU"
    worksheet_id = '527944025' # Base geral
    df = PDFExtractor.process_data_final(path, input_folder, output_folder, date)
    PDFExtractor.write_dataframe_google_sheets(path=path, spreadsheet_id=spreadsheet_id, worksheet_id=worksheet_id, df=df)
    PDFExtractor.move_files(source_folder=f'{path}/{input_folder}/', destination_folder=f'{path}/{destination_folder}/{date}') 


if __name__ == '__main__':
    main()