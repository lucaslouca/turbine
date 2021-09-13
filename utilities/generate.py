'''
EDGAR Application Programming Interfaces:
• https://www.sec.gov/edgar/sec-api-documentation
• https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json

'''

import csv
import json


def read_csv(file: str) -> list[dict]:
    result = []
    try:
        with open(file, newline='') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                result.append(row)
    except Exception as e:
        print(e)
    return result


def main():
    years = [y for y in range(2009, 2020)]
    concepts = ['CommonStockSharesOutstanding', 'EarningsPerShareDiluted', 'LongTermDebt']
    rows = read_csv(file='SP500.csv')
    tickers = sorted([d['Symbol'] for d in rows])

    # Concept Requests
    cd = {}
    cd['topic'] = 'concept'
    cd['resources'] = [{'tickers': tickers, 'concepts': [{'name': c, 'year': y} for c in concepts for y in years]}]
    concept_request = json.dumps(cd, indent=4)

    with open('in/sp500_concept_request.json', 'w') as file:
        file.write(concept_request)

    # Price Requests
    cd = {}
    cd['topic'] = 'price'
    cd['resources'] = [{'tickers': tickers, 'years': [y for y in years]}]
    prices_request = json.dumps(cd, indent=4)

    with open('in/sp500_prices_request.json', 'w') as file:
        file.write(prices_request)


if __name__ == "__main__":
    main()
