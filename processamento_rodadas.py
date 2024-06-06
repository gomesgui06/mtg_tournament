from utils import PDFExtractor
import pandas as pd


def main():
    path = "/home/gomes/Documentos/Gomes/extrator_dados_torneio_magic"
    input_folder = "torneios_para_processar/emparelhamento_por_nome"
    output_folder = f"resultado"
    destination_folder = f'torneios_processados'
    worksheet_id = '594884088' # planilha de junho e julho
    spreadsheet_id = "1zk93XeoQ6BgnEzH8NlKzakqjwXltNdODoHTtYowzFeU"
    date = PDFExtractor.format_date(f"{path}/{input_folder}/Rodada 1 Emparelhamentos por nome.pdf")  
    date_normal = PDFExtractor.format_date_normal(f"{path}/{input_folder}/Rodada 1 Emparelhamentos por nome.pdf")
    PDFExtractor.process_data_rodada(path, input_folder, output_folder, date)
    df = PDFExtractor.read_dataframes_in_folder(path, output_folder)
    df['resultado'] = df.apply(PDFExtractor.determinar_resultado, axis=1)
    
    df_decks = PDFExtractor.get_dataframe_google_sheets(path=path, spreadsheet_id=spreadsheet_id, worksheet_id='527944025')
    df_decks = df_decks[['jogadores', 'data', 'deck']]

    
    merged_df = pd.merge(df, df_decks, left_on=['jogador', 'data'], right_on=['jogadores', 'data'], how='left')
    merged_df = merged_df.drop(columns=['jogadores'])
    merged_df = merged_df.rename(columns={'deck': 'deck_jogador'})
    merged_df = pd.merge(merged_df, df_decks, left_on=['oponente', 'data'], right_on=['jogadores', 'data'], how='left')
    merged_df = merged_df.drop(columns=['jogadores'])
    merged_df = merged_df.rename(columns={
        'deck': 'deck_adversario', 
        'deck_jogador': 'deck', 
        'pontos_jogador': 'pontuacao', 
        'oponente': 'adversario', 
        'pontos_oponente': 'pontuacao_adversario'})
    merged_df = merged_df[['data', 'rodada', 'jogador', 'deck', 'pontuacao', 'resultado', 'adversario', 'deck_adversario', 'pontuacao_adversario']]
    PDFExtractor.write_dataframe_google_sheets(path=path, spreadsheet_id=spreadsheet_id, worksheet_id='3303320', df=merged_df)
    PDFExtractor.move_files(source_folder=f'{path}/{input_folder}/', destination_folder=f'{path}/{destination_folder}/{date}') 
if __name__ == '__main__':
    main()