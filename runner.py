from utility import *


cryptos = get_crypto_history_urls()

AllCurrencies = [d['Currency'] for d in cryptos if 'Currency' in d]
print(AllCurrencies)

CurrenciesToAnalyse = ['BTC-USD','ETH-USD']

'''
1. What is the average price of Bitcoin over the last 30 days?
2. What is the average price of Ethereum over the last 30 days?
'''
for i in CurrenciesToAnalyse:
    average = get_average(i,'Adj Close',days=30)
    print('Average Closing Price for currency '+i+' last 30 days: $'+str(average))


'''
3. What is the correlation between Bitcoin and Ethereum over the last 30 days?
'''
Correlation = get_correlation(CurrenciesToAnalyse,'Adj Close',days=30)
print('Correlation for currencies '+str(CurrenciesToAnalyse)+' last 30 days: '+str(Correlation))


'''
4. Are there any outliers in the data? If yes, what are they and why do you think
they are outliers?

Ans. Using the Scatter Plot below we can say that there are no outliers in the data 
     as there was no sudden/unexpected deviation of data points.
'''
for i in CurrenciesToAnalyse:
    get_scatter_plot(i,column='Adj Close')


'''
5. Are there any patterns in the data? If yes, what are they and why do you think
they are patterns?

Ans. Using the following Plot below we can say that there is a pattern. For either of the crypto currencies - when the price of one crypto currency falls 
it is likely that the price of other crypto currency has also fallen and vice versa
'''
get_plot_two(CurrenciesToAnalyse,column='Adj Close')

'''
6. Push all to s3
'''
for i in AllCurrencies:
     df = get_historical_data(i)
     path = './Currency/'+str(i)+'.csv'
     df.to_csv(path)
     uploaded = upload_to_aws(path, 'crypto-scrape', str(i)+'.csv')
    