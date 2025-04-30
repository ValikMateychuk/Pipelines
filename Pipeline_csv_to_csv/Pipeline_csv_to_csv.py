import pandas as pd


def extract(file_path):
    data = pd.read_csv(file_path)
    return data

def transform(data):
    clear_data = data.dropna()
    clear_data = clear_data[clear_data['age'] > 18]
    clear_data['name'] = clear_data['name'].str.upper()
    return clear_data

def load(data):
    data.to_csv('cleaned_data.csv', mode = 'w', index=False)
    return 'Data loaded successfully!'

extracted_data = extract('input.csv')
transformed_data = transform(extracted_data)
loaded_data = load(transformed_data)
print(loaded_data)