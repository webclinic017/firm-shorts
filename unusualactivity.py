import pandas as pd
import yfinance as yf
yf.pdr_override()  # i forget what this does

def unusual_activity(calls_or_puts, exp_date, stocklist):
    """
    unusualActivity scans yahoo finance for a list of stocks and returns contracts showing unusually high
    volume/open interest

    Args:
    calls_or_puts (str): do you want to return calls or puts?
    exp_date (str): date of contract expiry
    stocklist (list[str]): list of tickers to loop through
    """
    # We're going to suppress prints b/c datareader is annoying then restore printing so this helps
    finaldf = pd.DataFrame()
    for x in stocklist:
        ticker = yf.Ticker(x)
        print(ticker.ticker)
        try:
            opt = ticker.option_chain(exp_date)

            # NEED TO CONSOLIDATE THIS IF STATEMENT '-_-
            # why would anyone do this
            if calls_or_puts == 'calls':
                # Add some info about ticker and UL price to our dataframe
                opt.calls.insert(0, 'Symbol', x)
                opt.calls.insert(3, 'stock_price', data.get_data_yahoo(x, end=date.today())['Close'][-1])
                # Calculate our volume/open interest, this is how we define unusual activity
                opt.calls['V/OI'] = (opt.calls['volume'].astype('float') / opt.calls['openInterest'])
                # Inside of the brackets is where we apply our filter to get the unusual stuff.
                # Feel free to mess with this in your own version
                finaldf = finaldf.append(opt.calls[(opt.calls['volume'].astype('float') / opt.calls[
                    'openInterest'] > .5) & (opt.calls['openInterest'] > 200)])
            elif calls_or_puts == 'puts':
                # sys.stdout = open(os.devnull, "w")
                opt.puts.insert(0, 'Symbol', x)
                opt.puts.insert(3, 'stock_price', data.get_data_yahoo(x, end=date.today())['Close'][-1])
                opt.puts['stock_price'] = data.get_data_yahoo(x, end=date.today())['Close'][-1]
                opt.puts['V/OI'] = (opt.puts['volume'].astype('float') / opt.puts['openInterest'])
                finaldf = finaldf.append(opt.puts[(opt.puts['volume'].astype('float') / opt.puts[
                    'openInterest'] > .5) & (opt.puts['openInterest'] > 200)])
            else:
                print('set calls_or_puts equal to calls or puts retard')
                break
        except:
            print('cant get option chain')

    # final formatting changes
    finaldf = finaldf.drop(columns=['contractSymbol', 'lastTradeDate', 'contractSize', 'currency'])
    finaldf.insert(3, 'Distance OTM', finaldf['stock_price'] - finaldf['strike'])
    finaldf['Value'] = finaldf['openInterest'] * finaldf['lastPrice'] * 100

    return finaldf

# expiry = '2021-09-24'
# returned = Lurker.unusual_activity('calls', expiry, sp_list)
# print(returned)
# print(returned['Value'])
# returned.to_csv(f'./data/dynamic/stock/market/unusual_options_activity_{expiry}.csv')
