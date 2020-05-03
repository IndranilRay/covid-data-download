""" Download State Wise Covid-19 Recovery Data
Script download data in csv format from 'https://api.covid19india.org/states_daily.json'.
Script can download all three type of cases 1.Recovered, 2.Confirmed and 3.Deceased depending on case_type flag.

created_by: Indranil Ray
email: neal.welcome@gmail.com
"""
import gc
import os
import logging
import requests
import pandas as pd
from requests.exceptions import HTTPError

# configure logger
logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='a', format='%(levelname)s - %(message)s')
url = 'https://api.covid19india.org/states_daily.json'


states_dict = {}  # Dictionary for all the states as key and values set to empty list
states_list = []  # List of all the states
status_list = []  # Status type list
dates_list = []  # Dates as per provided in api
case_type = 'Confirmed'  # default case_type


def get_states_daily_data(source=url):
    """
    Function return dictionary data from provided source
    :param source:
    :return: dict
    """
    try:
        response = requests.get(url)
        json_data = response.json()
        if json_data.get('states_daily', None) is not None:
            data = json_data['states_daily']
        # raise for status
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred {http_err}')
        logging.error(f'HTTP error occurred {http_err}')
    except Exception as err:
        print(f'Other error occurred {err}')
        logging.error(f'HTTP error occurred {err}')
    else:
        logging.debug(f'Get request completed successfully with code {response.status_code}')
        return data


def init_data_frame(states_daily_dict=None):
    """
    Function create data frame from obtained json data
    :param states_daily_dict: Dictionary
    :return: pd.Dataframe()
    """
    # intialize data frame
    states_daily_df = pd.DataFrame(states_daily_dict)
    logging.debug(f'Set data frame from states_daily_dict ')
    # print(states_daily_dict)

    # set variables
    set_vars(states_daily_df=states_daily_df)
    return states_daily_df


def set_vars(states_daily_df=None):
    """
    Function set variables for further processing
    :param states_daily_df:
    :return:
    """
    try:
        global status_list, dates_list

        if isinstance(states_daily_df, pd.DataFrame):
            set_states_var(states_daily_list=states_daily_df.columns.tolist())
            status_list = states_daily_df.status.unique().tolist()
            # status_list = unique_status.copy()
            logging.info(f'status_list set')
            print(f'status_list {status_list}')

            dates_list = states_daily_df.date.unique().tolist()
            # dates_list = unique_dates.copy()
            logging.info(f'dates_list set')
            print(f'dates_list {dates_list}')
    except TypeError as type_error:
        logging.error(f'Expecting data frame object {type_error}')


def set_states_var(states_daily_list=None):
    """
    Function set states  dictionary and list
    :param states_daily_list:
    :return:
    """
    try:
        global states_list, states_dict
        if isinstance(states_daily_list, list):
            # remove date and status from list
            states_daily_list.remove('date')
            states_daily_list.remove('status')
            states_list = states_daily_list.copy()
            # print(f'state_list {states_list}')
            logging.debug(f'set states_list')

            for state in states_list:
                states_dict[state] = []
            logging.info(f'set state_dict')
            # print(f'state_dict {states_dict}')
    except TypeError as type_error:
        logging.error(f'Expecting list type data {type_error}')


def push_values_in_state_dict(daily_df=None, case_type=None):
    """
    Function iterate dates_list to get case_type data for states
    :param daily_df: pd.Dataframe()
    :param case_type: string
    :return:
    """
    logging.debug(f'push values in states_key')
    try:
        if isinstance(daily_df, pd.DataFrame):
            if case_type is None:
                raise ValueError
            for date in dates_list:
                rslt_df = daily_df[(daily_df['date'] == date) & (daily_df['status'] == case_type)]
                rslt_df.drop(['date', 'status'], axis=1, inplace=True)
                insert_values_in_state_dict(rslt_df)
    except TypeError as type_err:
        logging.debug(f'Expecting pandas data frame')
    except ValueError as value_err:
        logging.debug(f'Expecting case_type')


def insert_values_in_state_dict(rslt_df=None):
    """
    Function maps values from to df and append in state_dict
    :param rslt_df:
    :return:
    """
    global states_dict
    try:
        if isinstance(rslt_df, pd.DataFrame):
            result_df = rslt_df.to_dict('records')[0]
            # print(f'result_df {result_df}')
            for key in states_dict:
                if result_df.get(key, None) is not None:
                    states_dict[key].append(result_df.get(key))
            # print(states_dict['mh'])
    except TypeError as type_err:
        logging.error(f'Expecting pd.Dataframe type')


def replace_states_dict_keys(states_dict=None):
    """
    Functions replaces keys with state names
    :param states_dict: states dictionary with data
    :return: dict
    """
    try:
        if states_dict is None:
            raise ValueError

        states_keys = ['ANDAMAN AND NICOBAR', 'ANDHRA PRADESH', 'ARUNACHAL PRADESH', 'ASSAM', 'BIHAR', 'CHANDIGARH',
                       'CHATTISGARH', 'DAMAN AND DEU', 'DELHI', 'DADRA AND NAGAR HAVELI', 'GOA', 'GUJRAT', 'HIMACHAL PRADESH',
                       'HARAYANA', 'JHARKHAND', 'JAMMU AND KASHMIR', 'KARNATAKA', 'KERALA', 'LADAKH', 'LAKSHADWEEP',
                       'MAHARASHTRA', 'MEGHALAYA', 'MANIPUR', 'MADHYA PRADESH', 'MIZORAM', 'NAGALAND', 'ORISSA', 'PUNJAB',
                       'PUDUCHERRY', 'RAJASTHAN', 'SIKKIM', 'TELENGANA', 'TAMILNADU', 'TRIPURA', 'UTTAR PRADESH', 'UTTARAKHAND',
                       'WEST BENGAL']
        # changing keys of dictionary
        final_dict = dict(zip(states_keys, list(states_dict.values())))
        return final_dict
    except ValueError as value_err:
        logging.error(f'stated_dict cant be None {value_err}')


def download_daily_data():
    global states_dict
    states_daily_dict = get_states_daily_data(source=url)
    # print(states_daily_dict)

    # intialize data frame for further processing
    states_daily_df = init_data_frame(states_daily_dict=states_daily_dict)

    # append values in keys for states_dict
    push_values_in_state_dict(daily_df=states_daily_df, case_type=case_type)

    # replace states dict with keys
    states_dict = replace_states_dict_keys(states_dict)

    # convert dict into dataframe
    final_states_daily_df = pd.DataFrame.from_dict(states_dict, orient='index', columns=dates_list)

    # print(final_states_daily_df.head())
    file_name = case_type + '.csv'
    try:
        if os.path.exists('downloads') is False:
            os.makedirs('downloads')
        else:
            path = os.path.join('downloads', file_name)
            with open(path, "w"):
                final_states_daily_df.to_csv(path, index=True, header=True)
    except OSError as os_err:
        logging.error(f'Error creating directory {os_err}')
    finally:
        gc.collect()


if __name__ == "__main__":
    # states_daily_dict = get_states_daily_data(source=url)
    # # print(states_daily_dict)
    #
    # # intialize data frame for further processing
    # states_daily_df = init_data_frame(states_daily_dict=states_daily_dict)
    #
    # # append values in keys for states_dict
    # push_values_in_state_dict(daily_df=states_daily_df, case_type=case_type)
    #
    # # replace states dict with keys
    # states_dict = replace_states_dict_keys(states_dict)
    #
    # # convert dict into dataframe
    # final_states_daily_df = pd.DataFrame.from_dict(states_dict, orient='index', columns=dates_list)
    #
    # # print(final_states_daily_df.head())
    # file_name = case_type+'.csv'
    # path = os.path.join(file_name)
    #
    # final_states_daily_df.to_csv(path, index=True, header=True)
    # gc.collect()
    download_daily_data()





