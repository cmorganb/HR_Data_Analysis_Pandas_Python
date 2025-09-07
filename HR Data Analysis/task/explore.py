import pandas as pd
import requests
import os

def main():
    if not os.path.exists('../Data'):
        os.mkdir('../Data')

    # Stage 1
    a_data, b_data, hr_data = load_data()
    a_df, b_df, hr_df = modify_index(a_data, b_data, hr_data)

    # Stage 2
    df = merge_datasets(a_df, b_df, hr_df)

    # Stage 3
    df = df.sort_values(['average_monthly_hours', 'Department'], ascending=False)
    print(df.Department.head(10).to_list())

    filtered_df = df.query("Department == 'IT' & salary == 'low'")
    print(sum(filtered_df['number_project']))

    employees_to_check = ['A4', 'B7064', 'A3033']
    employee_check = df.loc[employees_to_check, ['last_evaluation', 'satisfaction_level']].values.tolist()

    print(employee_check)



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
    hr_df = hr_df.set_index('employee_id', drop=False)

    return a_df, b_df, hr_df

def merge_datasets(a_df, b_df, hr_df):
    """Receives three datasets (office A, office B and HR) and merges and sorts them"""

    df = pd.concat([a_df, b_df])
    df = df.merge(hr_df, left_index=True, right_index=True, indicator=True)
    df = df[df['_merge'] == 'both']
    df = df.drop(columns=['employee_office_id', 'employee_id', '_merge'])
    df = df.sort_index()

    return df


if __name__ == "__main__":
    main()
