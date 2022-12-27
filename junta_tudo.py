import glob
import os
import pandas as pd

enquads = pd.read_csv(
    'infracoes--ok.csv', 
    sep=';', 
    #usecols=['enquadramento']
)

enquadramentos = enquads['enquadramento'].\
    loc[enquads.Considerada == 'Sim'].tolist()

for year in range(2007,2022):
    path = './data/'
    ano = str(year)
    csv_files = glob.glob(os.path.join(path+ano, '*.csv'))

    df = pd.DataFrame()

    for f in csv_files:
        if ano in ['2019', '2020', '2021']:   
            try:    dflop = pd.read_csv(f, sep=';', low_memory=False)
            except: dflop = pd.read_csv(f, sep=';', encoding='ISO-8859-1', low_memory=False)
        else:
            try:    dflop = pd.read_csv(f, low_memory=False)
            except: dflop = pd.read_csv(f, sep=',', encoding='ISO-8859-1', low_memory=False)
        
        if len(df) > 0: df = pd.concat([df,dflop])
        if len(df) == 0: df = dflop.copy()
        
        print('Location:', f)
        print('File Name:', f.split('\\')[-1])
        print('n_Rows:', len(dflop))
        print('cum_Rows:', len(df), end='\n\n')
    
    rename = {
            'Número do Auto': 'numero_auto', 
            'Data da Infração (DD/MM/AAAA)' : 'dat_infracao',
            'Indicador de Abordagem' : 'tip_abordagem',  
            'Assinatura do Auto' : 'ind_assinou_auto', 
            'Indicador Veiculo Estrangeiro' : 'ind_veiculo_estrangeiro', 
            'Sentido Trafego' : 'ind_sentido_trafego', 
            'UF Placa' : 'uf_placa', 
            'UF Infração' : 'uf_infracao', 
            'BR Infração' : 'num_br_infracao', 
            'Km Infração' : 'num_km_infracao',  
            'Município' : 'nom_municipio', 
            'Código da Infração':'codigo', 
            'Descrição Abreviada Infração' : 'descricao_abreviada', 
            'Enquadramento da Infração' : 'enquadramento', 
            'Início Vigência da Infração' :'data_inicio_vigencia', 
            'Fim Vigência Infração' : 'data_fim_vigencia', 
            'Medição Infração' : 'med_realizada', 
            'Descrição Especie Veículo' : 'especie', 
            'Descrição Marca Veículo' : 'nome_veiculo_marca', 
            'Hora Infração' : 'hora',
            'Medição Considerada' : 'med_considerada', 
            'Excesso Verificado' : 'exc_verificado'
    }

    df = df.rename(rename, axis=1)
    

    #df.dat_infracao = pd.to_datetime(df.dat_infracao)
    #df['mes_infracao'] = df.dat_infracao.dt.month
    #df['ano_infracao'] = df.dat_infracao.dt.year

    try:    df.drop(['hora'], axis=1, inplace=True)
    except: pass
    try:    df.drop(['numero_auto'], axis=1, inplace=True)
    except: pass
    try:    df.drop(['codigo'], axis=1, inplace=True)
    except: pass
    try:    df.drop(['nom_modelo_veiculo'], axis=1, inplace=True)
    except: pass

    cols = ['dat_infracao', 
            #'nom_municipio', 
            'uf_infracao', 
            #'descricao_abreviada', 
            'med_considerada']

    df = df.loc[df.enquadramento.isin(enquadramentos)]
    
    df = df[cols]
    
    _path = './csv_v2'
    name ='infracoes_'
    extension = '.csv'
    savePath = os.path.join(_path, name+ano+extension)

    print('Exportando CSV...', end='\n\n')
    df.to_csv(savePath, index=False, sep=';')
