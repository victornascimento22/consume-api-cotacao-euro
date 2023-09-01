import requests
import oracledb
import datetime
import schedule
import time

def req():
    data_atual = datetime.date.today().strftime('%m-%d-%Y')
    data_hora = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Requisição feita em {data_hora}")
    url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodoFechamento(codigoMoeda=@codigoMoeda,dataInicialCotacao=@dataInicialCotacao,dataFinalCotacao=@dataFinalCotacao)?%40codigoMoeda='EUR'&%40dataInicialCotacao='{data_atual}'&%40dataFinalCotacao='{data_atual}'"

    response = requests.get(url)
    
    connection = None
    cursor = None  
    
    if response.status_code == 200:
        data = response.json()
       
        try:
            connection = oracledb.connect(user="dwu", password="S7hHmdmz28i2", host="cloud.upquery.com", port=1521, service_name="capital")
        
            cursor = connection.cursor()
    
            for item in data['value']:
                cotacaoCompra = str(item['cotacaoCompra'])
                cotacaoVenda = str(item['cotacaoVenda'])
                dataHoraCotacao = item['dataHoraCotacao']  # A data e hora em formato de string
                tipoBoletim = item['tipoBoletim']
    
                sql = "INSERT INTO ETL_OLINDA_EURO (cotacaoCompra, cotacaoVenda, dataHoraCotacao) VALUES (:1, :2, :3)"
    
                cursor.execute(sql, (cotacaoCompra, cotacaoVenda, dataHoraCotacao))
                connection.commit()
                
            print("Dados inseridos com sucesso no Oracle.")
            
        except Exception as e:
            
            print(f"Erro ao inserir dados no banco de dados Oracle: {str(e)}")
            
        finally:
            
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        
        print("Erro na requisição. Código de status:", response.status_code)


schedule.every().day.at("13:20").do(req)


while True:
    schedule.run_pending()
    time.sleep(1)
