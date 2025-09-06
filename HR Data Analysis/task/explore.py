import pandas as pd
import requests
import os

def main():
    if not os.path.exists('../Data'):
        os.mkdir('../Data')


    a_data, b_data, hr_data = load_data()

    a_df, b_df, hr_df = modify_index(a_data, b_data, hr_data)

def load_data():
    """Downloads the data and creates the necessary DataFrames"""

    # Download data if it is unavailable.
    if ('A_office_data.xml' not in os.listdir('../Data') and
        'B_office_data.xml' not in os.listdir('../Data') and
        'hr_data.xml' not in os.listdir('../Data')):
        print('A_office_data loading.')
        url = "https://www.dropbox.com/s/jpeknyzx57c4jb2/A_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/A_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('B_office_data loading.')
        url = "https://www.dropbox.com/s/hea0tbhir64u9t5/B_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/B_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('hr_data loading.')
        url = "https://www.dropbox.com/s/u6jzqqg1byajy0s/hr_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/hr_data.xml', 'wb').write(r.content)
        print('Loaded.')

        # All data in now loaded to the Data folder.

    a_df = pd.read_xml('../Data/A_office_data.xml')
    b_df = pd.read_xml('../Data/B_office_data.xml')
    hr_df = pd.read_xml('../Data/hr_data.xml')

    return a_df, b_df, hr_df

def modify_index(a_df, b_df, hr_df):
    """Receives three DataFrames and modifies their indexes to remove ambiguities"""

    a_df.index = ['A' + str(i) for i in a_df['employee_office_id']]
    b_df.index = ['B' + str(i) for i in b_df['employee_office_id']]
    hr_df = hr_df.set_index('employee_id')

    print(a_df.index.to_list())
    print(b_df.index.to_list())
    print(hr_df.index.to_list())

    return a_df, b_df, hr_df


if __name__ == "__main__":
    main()
