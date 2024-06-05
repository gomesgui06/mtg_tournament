from utils import PDFExtractor


def main():
    path = "/home/gomes/Documentos/Gomes/extrator_dados_torneio_magic"
    input_folder = "torneios_para_processar/emparelhamento_por_nome"
    output_folder = f"resultado"
    destination_folder = f'torneios_processados'
    worksheet_id = '594884088' # planilha de junho e julho
    spreadsheet_id = "1zk93XeoQ6BgnEzH8NlKzakqjwXltNdODoHTtYowzFeU"
    date = PDFExtractor.format_date(f"{path}/{input_folder}/Rodada 1 Emparelhamentos por nome.pdf")  
    PDFExtractor.process_data_rodada(path, input_folder, output_folder, date)
    df = PDFExtractor.read_dataframes_in_folder(path, output_folder)
    df['resultado'] = df.apply(PDFExtractor.determinar_resultado, axis=1)
    PDFExtractor.write_dataframe_google_sheets(path=path, spreadsheet_id=spreadsheet_id, worksheet_id=worksheet_id, df=df)
    # PDFExtractor.move_files(source_folder=f'{path}/{input_folder}/', destination_folder=f'{path}/{destination_folder}/{date}') 

# ToDO
# - Pegar o nome do deck a partir da planilha
# - Salvar os arquivos processados no drive
if __name__ == '__main__':
    main()