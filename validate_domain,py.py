import pandas as pd
import validators
from validators import ValidationFailure

def is_string_an_url(url_string):
    result = validators.url(url_string)

    if isinstance(result, ValidationFailure):
        return False

    return result


pd.set_option('display.max_columns', None)
df = pd.read_csv('AllDomainsMerged.csv')
df = df.iloc[:3]
#print(df['input_domain'])
# domain = df['input_domain']
# print(domain)
df['Validation'] = df['input_url'].map(lambda x : is_string_an_url(x) )

print(df)

df.to_csv("validated.csv")
#print(is_string_an_url('thclabelsolutions.com'))

