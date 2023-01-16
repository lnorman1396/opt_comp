import requests
import json
import logging
import time
import streamlit as st
import re
import pandas as pd 
from pandas import  ExcelWriter
from io import BytesIO
from datetime import date
import numpy as np
import os
from os import getcwd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta
import streamlit_ext as ste
import secrets 
import pydeck as pdk
import random
import string
# import module
from geopy.geocoders import Nominatim
import  streamlit_toggle as tog
import streamlit_nested_layout
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import os
from matplotlib.backends.backend_agg import RendererAgg
import plotly.express as px

import hashlib







st.set_page_config(page_title=None, layout='wide')

emea_str = 'Albania, Andorra, Armenia, Austria, Azerbaijan, Belarus, Belgium, Bosnia and Herzegovina, Bulgaria, Croatia, Cyprus, Czech Republic, Denmark, Estonia, Faroe Islands, Finland, France, France, Metropolitan, Georgia, Germany, Gibraltar, Greece, Greenland, Hungary, Iceland, Ireland, Italy, Kazakhstan, Kyrgyzstan, Latvia, Liechtenstein, Lithuania, Luxembourg, Macedonia, Malta, Moldova, Monaco, Netherlands, Norway, Poland, Portugal, Romania, Russian Federation, San Marino, Serbia and Montenegro, Slovakia, Slovenia, Spain, Svalbard and Jan Mayen Islands, Sweden, Switzerland, Tajikistan, Turkey, Turkmenistan, Ukraine, United Kingdom, Uzbekistan, Vatican, Algeria, Angola, Benin, Botswana, Bouvet Island, Burkina Faso, Burundi, Cameroon, Cape Verde, Central African Republic, Chad, Comoros, Congo, Congo, Democratic Republic, Cote d’Ivoire, Djibouti, Egypt, Equatorial Guinea, Eritrea, Ethiopia, Gabon, Gambia, Ghana, Guinea, Guinea-Bissau, Kenya, Lesotho, Liberia, Libya, Madagascar, Malawi, Mali, Mauritania, Mauritius, Mayotte, Morocco, Mozambique, Namibia, Niger, Nigeria, Oman, Rwanda, Sao Tome and Principe, Senegal, Seychelles, Sierra Leone, Somalia, South Africa, Swaziland, Tanzania, Togo, Tunisia, Uganda, Western Sahara, Zambia, Zimbabwe, Bahrain, British Indian Ocean Territory, Iran, Iraq, Israel, Jordan, Kuwait, Lebanon, Palestinian Territory, Qatar, Reunion, Saudi Arabia, United Arab Emirates, Yemen'


# scope of the application
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

#Get current working directory
cwd = os.getcwd()
#get json credentials for service account through Cred.json file 

#credentials = ServiceAccountCredentials.from_json_keyfile_name(cwd+'/Cred.json', scope)


#TODO: write a function to cache data from api sheet if no change - can use the 304 code from etag to check no change, this should speed up request from TOML if no change. 


#TODO: Implement caching function on widgets so reruns

d = st.secrets['google_sheets']



credentials = ServiceAccountCredentials.from_json_keyfile_dict(d)
#Pass through credentials to access file
client = gspread.authorize(credentials)


# Open the spreadhseet and create a sheet variable 
sheet = client.open("scriptanalyticsoptibus").worksheet("test_meta")
#Open other sheet for archived records 
sheet_archive = client.open("scriptanalyticsoptibus").worksheet("Archives")


#get all records - returns a list of dictionaries
data = sheet.get_all_records()
#Get all archived records - //
data_archives = sheet_archive.get_all_records()

#Dictionary mapping domain-substring to client value (will be used in df mapping later on)

#TODO: may need other client names - only have partial set so far 

clients_dict = {
    'arriva-uk-bus': 'Arriva UK', 
    'sg': 'Stagecoach', 
    'firstbus': 'First Bus'
}


#Tabs list
names_of_tabs = ['Dashboard','Submit Schedule to Records','Read Record Data']
#two tabs for two sections of apps which calls the tab list for string content 
tab0, tab1, tab2 = st.tabs(names_of_tabs)




api_secrets_dict = st.secrets['api_secrets_dict']



#function to split URL into three substrings
def process_URL(schedule_URL):
    domain_name = re.sub("\.[^.]*", "", schedule_URL[8:])
    schedule_id = schedule_URL
    schedule_id = re.sub(r'^.*?(?=schedules/)', "", schedule_id)
    schedule_id = schedule_id[10:].split('/', -1)[0]
    project_id = schedule_URL
    project_id = re.sub(r'^.*?(?=project/)', "", project_id)
    project_id = project_id[8:].split('/', -1)[0]
    return domain_name, schedule_id , project_id

#Function to get client_id and secret based on the domain name key pasted in the URL
def generate_auth(domain_name, api_secrets_dict):
    client_id = api_secrets_dict[domain_name]["client_id"]
    client_secret = api_secrets_dict[domain_name]["client_secret"]
    return client_id, client_secret

#Function to use auth server endoiunt to get new token 
def get_new_token(client_id, client_secret, domain_name, a):
    auth_server_url = f"https://{domain_name}.optibus.co/api/v2/token"
    token_req_payload = {'grant_type': 'client_credentials'}
    if domain_name != "":
        token_response = requests.post(auth_server_url,
        data=token_req_payload, verify=False, allow_redirects=False,
        auth=(client_id, client_secret))          
        if token_response.status_code !=200:
            col1, col2 = st.columns([8,2])
            with col1:
                st.error(f"Failed to obtain token from the OAuth 2.0 server **{token_response.status_code}**")
            with col2:
                rerun = st.button('Retry')
                if rerun: 
                    st.experimental_rerun()
                else:
                    st.stop()
        else:
            #st.success(f"Successfuly obtained a new token for **{a} Schedule**")
            tokens = json.loads(token_response.text)
            return tokens['access_token']
    else:
        st.stop()

#function that uses generated token from function above in get request for api (uses the token in header)
#returns json in variable 

def api_header_response(token, domain_name, schedule_id):
    api_call_headers = {'Authorization': 'Bearer ' + token}
    api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedule/{schedule_id}?needStats=true', headers=api_call_headers, verify=False)

    get_json = api_call_response.json()
    return get_json

def get_days_of_week(get_json):
    dow = get_json['service']['daysOfWeek']
    return dow
def get_optibus_id(get_json):
    opId = get_json['scheduleSet']['optibusId']
    return opId 
def api_services_response(token, domain_name, optibus_id):
    api_call_headers = {'Authorization': 'Bearer ' + token}
    api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedule/{optibus_id}/services', headers=api_call_headers, verify=False)
    get_services_json = api_call_response.json()
    return get_services_json

#NEW FUNCTION TO FILTER OUT NWD 
def create_json_list(get_services_json, token, domain_name):
    emp_list = []
    exclude = ['NWD', '#SCH', 'NSCH']
    for d in get_services_json:
        if not any(substring in d['name'] for substring in exclude):
            emp_list.append(api_header_response(token, domain_name, d['id']))
    return emp_list




def create_paid_time_list(json_data_list):
    # List to store the results
    paid_time_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']

        # Get the length of the list
        list_length = len(list_value)

        # Multiply the value of the 'other_key' key by the length of the list
        result = sch_d['service']['stats']['crew_schedule_stats']['paid_time'] * list_length

        # Append the result to the result list
        paid_time_list.append(result)

    paid_time_list_sum = sum(paid_time_list)
    return paid_time_list, paid_time_list_sum
def create_platform_time_list(json_data_list):
    # List to store the results
    platform_time_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']

        # Get the length of the list
        list_length = len(list_value)

        # Multiply the value of the 'other_key' key by the length of the list
        result = sch_d['service']['stats']['vehicle_schedule_stats']['platform_time'] * list_length

        # Append the result to the result list
        platform_time_list.append(result)

    paid_time_list_sum = sum(platform_time_list)
    return platform_time_list, paid_time_list_sum
def create_duty_count_list(json_data_list):
    # List to store the results
    duty_count_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']

        # Get the length of the list
        list_length = len(list_value)

        # Multiply the value of the 'other_key' key by the length of the list
        result = sch_d['service']['stats']['crew_schedule_stats']['duties_count'] * list_length

        # Append the result to the result list
        duty_count_list.append(result)

    duty_count_list_sum = sum(duty_count_list)
    return duty_count_list, duty_count_list_sum


#return individual values from json dict
def get_duties(get_json):
    duty_count = get_json['service']['stats']['crew_schedule_stats']['duties_count']
    return duty_count

#return individual values from json dict
def get_paid_time(get_json):
    paid_time = get_json['service']['stats']['crew_schedule_stats']['paid_time']
    return paid_time


def create_split_count_list(json_data_list):
    # List to store the results
    split_count_list = [sch_d['service']['stats']['crew_schedule_stats']['split_count'] * len(sch_d['service']['daysOfWeek']) for sch_d in json_data_list]

    split_count_list_sum = sum(split_count_list)
    return split_count_list, split_count_list_sum
def create_paid_break_time_list(json_data_list):
    result = []
    for d in json_data_list:
        inner_list = []
        for l in d['service']['stats']['crew_schedule_stats']['custom_time_definitions']:
            if l['name']=='Paid Break':
                inner_list.append(l['value'])
        result.append(sum(inner_list))

    result_with_service = sum([x * y for x, y in zip(result, [len(sch_d['service']['daysOfWeek']) for sch_d in json_data_list])])

    return result_with_service

#calculation of avg paid time
def calculate_avg_paid_time(paid_time, duty_count):
    avg_paid_time = paid_time/duty_count
    return avg_paid_time

#return individual values from json dict
def get_platform_time(get_json):
    platform_time = get_json['service']['stats']['vehicle_schedule_stats']['platform_time']
    return platform_time

#Calculation of schedule efficiency (FUNCTION SHOULD REALLY BE CHANGED TO calculate_sch_eff)
def get_sch_eff(platform_time, paid_time):
    efficiency = (platform_time/paid_time)*100
    return efficiency

#Calculation of efficiency difference 
def calculate_eff_diff(efficiency_ba, efficiency_op):
    eff_diff = round(efficiency_op - efficiency_ba, 2)
    return eff_diff

#Calculation of duty count difference
def calculate_duty_diff(duty_count_ba, duty_count_op):
    duty_count_diff = int(duty_count_ba - duty_count_op)
    return duty_count_diff

#Calculation of paid time difference 
def calculate_paid_time_diff(paid_time_ba, paid_time_op):
    pt_diff = paid_time_ba - paid_time_op
    return pt_diff

#Converting minutes into a HH:MM string time format
def minutes_to_hours(minutes):
    # Calculate the number of hours
    hours = int(minutes // 60)
    # Calculate the number of remaining minutes
    remaining_minutes = int(minutes % 60)
    # Return the hours and minutes as a string, separated by a colon
    return f"{hours}:{remaining_minutes:02d}"

#Function to concatenate the values of one list with the values of another and return two lists, project name and also the concatenated list
def get_values(dict_list, key1, key2):
    return [d[key1] for d in dict_list], [d[key1] +' - '+ d[key2] for d in dict_list]

#get index of where deleted row should be 
def get_index(dict_list, key, value):
    for i, d in enumerate(dict_list):
        if d[key] == value:
            return i
    return -1

def get_depot_from_api(get_json):
    depot_item = []
    for d in get_json['stats']['vehicle_schedule_stats']['depot_allocations']:
        depot_item.append(d)
    return depot_item[0][0]
def get_stop_details_from_depot_id(get_json, depot_item):
    stop = get_json['stops']
    list_stop_dict = []
    for d in stop:
        if d['id']==depot_item:
            list_stop_dict.append(d)
    dict_depot = list_stop_dict[0]
    stop_name = dict_depot['name']
    lat = dict_depot['lat']
    long = dict_depot['long']
    return dict_depot ,stop_name, lat, long

def get_region_from_country(string_of_values, string_to_assign_region, country):
    list_to_find_region = string_of_values.split(", ")
    if country in list_to_find_region:
        region = string_to_assign_region
    else:
        region = 'Other'
    return region

#TODO: apply key error logic to other params that aren't essential, maybe not insert 0 bt inform user and insert a manual option? - in new form? - Manual Mode

def create_generic_time_stat_list(json_data_list, string, string2, key_val):
    # List to store the results
    generic_time_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']

        # Get the length of the list
        list_length = len(list_value)

        try:
        # Multiply the value of the 'other_key' key by the length of the list
            result = sch_d['service']['stats'][key_val][string] * list_length
        except KeyError:
            result = 0
            st.warning(f'We have had to assume this {string} is 0 due to key not being found on these Days {list_value} on {string2}')

        # Append the result to the result list
        generic_time_list.append(result)

    generic_list_sum = sum(generic_time_list)
    return  generic_list_sum     


with tab0:
    

    colhead1, colhead2, colhead3, colhead4 = st.columns([2,5,5,2])
    colhead1.write('')
  
    colhead1.subheader(':blue[Dashboard]')

    #client_list = list(set([d['Client'] for d in data]))
    
    #'PVR', 'Efficiency Difference', 'Duty Count'
    generic_list = ['Client','Region', 'Country', 'Domain']
    generic_selection = colhead2.selectbox(label='', options=generic_list, key='g', help= 'Select Category to filter by')

    def define_initial_filter(generic_selection):
        return ['All'] + list(set([d[generic_selection] for d in data]))

    initial_filter_list = define_initial_filter(generic_selection)
    #client_list = ['All'] + list(set([d['Client'] for d in data]))
    #region_list = ['All'] + list(set([d['Region'] for d in data]))
    #country_list = ['All'] + list(set([d['Country'] for d in data]))

    dashboard_selection = colhead3.selectbox(label='', options=initial_filter_list, key='ab', help= 'Select Sub-Category to filter by, this list is based on your first selection')
    
    if dashboard_selection and dashboard_selection != 'All':
        filtered_data = [d for d in data if d[generic_selection] == dashboard_selection]
    else:
        filtered_data = data


    further_filter_list = ['None', 'PVR', 'Efficiency Difference', 'Optimisation Duty Count']
    further_selection = colhead4.selectbox(label='', options=further_filter_list, key='abcd', help = 'Select further filter to access data quantiles' )

    if further_selection != 'None':
        st.info(f"Filtering data by **{further_selection}** *(these are presented in their respective quantiles based on min and max values from the dataset*)")
        min_value = min([d[further_selection] for d in data])
        max_value = max([d[further_selection] for d in data])
        threshold = np.percentile([d[further_selection] for d in data], 100)
        bin_size = round((max_value - min_value) / 3)
        bins = [f"0 - {min_value:.0f}",
                        f"{min_value:.0f} - {min_value + bin_size:.0f}",
                        f"{min_value + bin_size:.0f} - {min_value + 2*bin_size:.0f}",
                        f"{min_value + 2*bin_size:.0f} - {max_value:.0f}",
                        f"over {max_value:.0f}"]
        bin_selection = st.radio("Select bin", bins, index=1, horizontal=True, label_visibility='collapsed')

        # Extract the selected bin range
        if bin_selection.startswith("over"):
            filtered_data = [d for d in filtered_data if d[further_selection] >= threshold]
        else:
            bin_range = [int(x) for x in bin_selection.split("-")]
            filtered_data = [d for d in filtered_data if bin_range[0] <= d[further_selection] < bin_range[1]]

    if not filtered_data:
        st.info("No data matches this filter. Please select a different filter.")
        st.stop()



        
    
        

    #percentage = colhead3.checkbox('as %')
    


    def sum_numeric_values(data, value1, value2):
        total1 = sum(d[value1] for d in data)#basline
        total2 = sum(d[value2] for d in data)#optimsation
        delta =  total2 - total1
        
        
        delta2 = round((delta/total1)*100,2)
        
        
        delta = f"{int(delta)//60}:{int(delta)%60:02d}" if isinstance(delta, (int, float)) else delta
        total1 = f"{int(total1)//60}:{int(total1)%60:02d}" if isinstance(total1, (int, float)) else total1
        total2 = f"{int(total2)//60}:{int(total2)%60:02d}" if isinstance(total2, (int, float)) else total2
        
        return total1, total2, delta, delta2

        

    def sum_count_values(data, value1, value2):
        total1 = sum(d[value1] for d in data)
        total2 = sum(d[value2] for d in data)
        delta =  total2 - total1 
        return total1, total2, delta

    def calculate_efficiency(data, pt_1, pt_2, pl_1, pl_2):
        pt_2_1 = sum(d[pt_2] for d in data)
        pl_2_1 = sum(d[pl_2] for d in data)
        total_bs = (sum(d[pl_1] for d in data))/ (sum(d[pt_1] for d in data))
        total_op = (sum(d[pl_2] for d in data))/ (sum(d[pt_2] for d in data))
        op_eff = round((pl_2_1/pt_2_1)*100, 2)





        #round(((df[opt_platform_time].iloc[0]/df[opt_paid_time].iloc[0])-(df[baseline_platform_time].iloc[0]/df[baseline_paid_time].iloc[0]))*100,2)
        #TODO: NEED TO RECALCULATE DELTA 
        delta_eff = round((total_op - total_bs)*100, 2)
        return delta_eff, op_eff

    def assign_rgb(df):
        import random
        # Create a dictionary to store the mapping of client values to RGB values
        client_to_rgb = {}

        # Iterate over the unique client values
        for client in df['Client'].unique():
            # If the client value has not been seen before, generate a new random RGB value and store it in the dictionary
            if client not in client_to_rgb:
                client_to_rgb[client] = [random.randint(70, 255), random.randint(70, 255), random.randint(70, 255), 255]
        df['rgb'] = df['Client'].map(client_to_rgb)

        return df

    def generate_rgb(df_len):
        return np.random.randint(low=70, high = 200, size = (df_len, 3))

    delta_eff, op_eff = calculate_efficiency(filtered_data, 'Baseline Paid Time', 'Optimisation Paid Time', 'Baseline Platform Time', 'Optimisation Platform Time')

    

    pt_sum_ba, pt_sum_op, pt_del, pt_del2 = sum_numeric_values(filtered_data, 'Baseline Paid Time', 'Optimisation Paid Time')
    pb_sum_ba, pb_sum_op, pb_del, pb_del2 = sum_numeric_values(filtered_data, 'Baseline Paid Break Time', 'Optimisation Paid Break Time')
    sp_sum_ba, sp_sum_op, sp_del = sum_count_values(filtered_data, 'Baseline Split Count', 'Optimisation Split Count')
    dc_sum_ba, dc_sum_op, dc_del = sum_count_values(filtered_data, 'Baseline Duty Count', 'Optimisation Duty Count')

    
    with st.expander('**KPIS**', expanded= True):
        col1, col2, col3, col4, col5 = st.columns([2,2,2,2,1])
        col1.metric('**Efficiency**', value=op_eff, delta = f'{delta_eff}%')
        col2.metric('**Paid Time**', value=pt_sum_op, delta =f'{pt_del2}% ({pt_del})',  delta_color="inverse")
        col3.metric('**Paid Break**', value=pb_sum_op, delta=f'{pb_del2}% ({pb_del})', delta_color="inverse")
        #TODO: Create a function to return delta colour based on int value to 0, this should be applied to all values of integers including duty count,spliy, changover ect
        if dc_del ==0:
            delta_colour_dc = 'off'
        else:
            delta_colour_dc = 'inverse'
        col4.metric('**Duty Count**', value=dc_sum_op, delta=f'{dc_del}', delta_color=delta_colour_dc)

        col5.metric('**Schedules**', value = f'{len(filtered_data)}/{len(data)}')
        
        with st.expander('Further KPIS'):
            col1k, col2k, col3k, col4k = st.columns(4)
            if sp_del ==0:
                delta_colour = 'off'
            else:
                delta_colour = 'inverse'
            col1k.metric('**Split Count**', value=sp_sum_op, delta = sp_del, delta_color=delta_colour)

            #TODO: Standby Time stat in here 
            #TODO: Spread Time stat in here 
            #TODO: AVG PAid Time 
            #TODO: Changeovers 
            #TODO: Relief Cars

    plot_dataframe = pd.DataFrame(filtered_data, columns= ['Latitude','Longitude', 'Optimisation Duty Count', 'Project Name', 'Paid Time Difference', 'Efficiency Difference' , 'Duty Count Difference', 'Client', 'PVR'])
    plot_dataframe = plot_dataframe.rename(columns={"Optimisation Duty Count": "Opt",'Paid Time Difference':'PTD', 'Efficiency Difference':'EFD', 'Project Name': 'PRN', 'Duty Count Difference':'DCS' })

    plot_dataframe_elevation = plot_dataframe['Opt']


    plot_dataframe['Opt2'] = plot_dataframe['Opt'].apply(lambda d: f'{round(d, 2):,}')
    plot_dataframe['EFD2'] = plot_dataframe['EFD'].apply(lambda d: f'{d}'+'%')
    def format_minutes(minutes):
        hours = minutes // 60
        minutes = minutes % 60
        return f"{hours}:{minutes:02d}"
    plot_dataframe['PTDH'] = plot_dataframe['PTD'].apply(format_minutes)
    plot_dataframe['PTDHr'] = plot_dataframe['PTD'].apply(lambda x: x / 60)




    plot_dataframe = assign_rgb(plot_dataframe)



    rgb_values = generate_rgb(len(plot_dataframe))
    rgb_values.tolist().append(255)
    plot_dataframe['rgb'] = rgb_values



    main_column_1, main_column_2 = st.columns([60, 40])
    with main_column_1:
        with st.expander('**Map Options**', expanded=True):
             #TODO: PUT ALL OPTIONS IN HERE EVEN COLOUR PALLETE CONFIG AND MAP LAYER VIEWS, SHOULD BE COLLAPSED
            data_type = st.radio('Filter map Elevations By:', ('PVR', 'Duty Count', 'Paid Time Difference', 'Efficiency Difference'), horizontal=True)
            color_type = st.radio('Generate Colours By', ('Client', 'PVR', 'Number of Duties', 'Random'), horizontal=True)

        if color_type == 'Random':
            rgb_values = generate_rgb(len(plot_dataframe))
            plot_dataframe['rgb'] = rgb_values.tolist()

        elif color_type == 'Client':
            plot_dataframe = assign_rgb(plot_dataframe)
            #plot_dataframe['hex'] = plot_dataframe['rgb'].apply(lambda x: '#{:02x}{:02x}{:02x}{:02x}'.format(*x))
            plot_dataframe['hex'] = plot_dataframe['rgb'].apply(lambda x: '#' + ''.join([hex(i)[2:].zfill(2) for i in x[:3]]))
            #plot_dataframe['hex'] = plot_dataframe['rgb'].apply(lambda x: '#'+''.join([hex(i)[-2:] for i in x]) )
            plot_dataframe_a = plot_dataframe.drop_duplicates(subset=['hex'], keep='first')
            plot_dataframe_a['hex'] = plot_dataframe_a['hex'].str.upper()
            # Create a dictionary of hex color codes and their corresponding client
            #color_map = dict(zip(plot_dataframe_a.Client, plot_dataframe_a.hex))

           
            with st.expander('Colour Reference', expanded=False):
                cols = st.columns(len(plot_dataframe_a))
                for i in range(len(plot_dataframe_a)):
                    with cols[i]:
                        st.color_picker(label=plot_dataframe_a.iloc[i]['Client'], value=plot_dataframe_a.iloc[i]['hex'], key=''.join(random.choice(string.ascii_lowercase) for i in range(7)))

        elif color_type == 'Number of Duties':
            custom_colour = st.color_picker(label= '**Please select the colour you wish to present**', value='#E6D0D0')
            import struct
            match_colour = {
                            'Under_250':50, 
                            '250_500':100,
                            '500_750':150,
                            '750_1000':200, 
                            'over_1000':250
                            }
            def hex_to_rgba(hex_string, number_of_duties, match_colour):
                hex_string = hex_string.lstrip('#')
                if len(hex_string) == 3:
                    hex_string = ''.join([c * 2 for c in hex_string])
                r, g, b = struct.unpack('BBB', bytes.fromhex(hex_string))
                if number_of_duties < 250:
                    return [r, g, match_colour['Under_250'], 255]
                elif number_of_duties < 500:
                    return [r, g, match_colour['250_500'],255]
                elif number_of_duties < 750:
                    return [r, g, match_colour['500_750'],255]
                elif number_of_duties < 1000:
                    return [r, g, match_colour['750_1000'],255]
                else:
                    return [r, g, match_colour['over_1000'], 255]

                    #Would be good to have a fixed columnised system under the expander where you can present these colours with respective ranges 

            plot_dataframe['rgb'] = plot_dataframe.apply(lambda row: hex_to_rgba(custom_colour, row['Opt'], match_colour), axis=1)

        elif color_type == 'PVR':
            custom_colour = st.color_picker(label= '**Please select the colour you wish to present**', value='#E6D0D0')
            import struct
            match_colour = {
                            'Under_25':90, 
                            '25_50':125,
                            '50_75':175,
                            '75_100':210, 
                            'over_100':255
                            }
            def hex_to_rgba(hex_string, number_of_duties, match_colour):
                hex_string = hex_string.lstrip('#')
                if len(hex_string) == 3:
                    hex_string = ''.join([c * 2 for c in hex_string])
                r, g, b = struct.unpack('BBB', bytes.fromhex(hex_string))
                if number_of_duties < 25:
                    return [r, g, b , match_colour['Under_25']]
                elif number_of_duties < 50:
                    return [r, g, b, match_colour['25_50']]
                elif number_of_duties < 75:
                    return [r, g, b , match_colour['50_75']]
                elif number_of_duties < 100:
                    return [r, g, b ,match_colour['75_100']]
                else:
                    return [r, g, b , match_colour['over_100']]

                    #Would be good to have a fixed columnised system under the expander where you can present these colours with respective ranges 

            plot_dataframe['rgb'] = plot_dataframe.apply(lambda row: hex_to_rgba(custom_colour, row['PVR'], match_colour), axis=1)
            
    


            
        if data_type == 'PVR':
            elevation = 'PVR'
            elevation_scale_val= 900
            colour_val = [255, 165, 0, 80]
        elif data_type == 'Duty Count':
            elevation = 'Opt'
            elevation_scale_val= 100
            colour_val = [255, 165, 0, 80]
        elif data_type == 'Paid Time Difference':
            elevation = 'PTDHr'
            elevation_scale_val = 400
            colour_val= [22, 196, 48, 60]

        elif data_type == 'Efficiency Difference':
            elevation = 'EFD'
            elevation_scale_val = 13000
            colour_val= [81, 216, 240, 60]
        midpoint = (np.average(plot_dataframe['Latitude']), np.average(plot_dataframe['Longitude']))

        st.info(f'A Map showing the distribution in elevation of **{data_type}** using **{dashboard_selection}** Client(s)')
        #TODO: MAYBE PROVIDE MORE MAP OPTIONs, and EVEN OTHER LAYER CONFIGURATIONS? 
        satelite = st.checkbox('satelite', key='sate')
        if satelite:
            map_provider="mapbox"
            map_style = pdk.map_styles.MAPBOX_SATELLITE
        else:
            map_provider=None
            map_style=pdk.map_styles.CARTO_DARK

            # view (location, zoom level, etc.)
        view = pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], pitch=50, zoom=3)

        # layer
        column_layer = pdk.Layer('ColumnLayer',
                                data=plot_dataframe,
                                get_position=['Longitude', 'Latitude'],
                                get_elevation=elevation,
                                elevation_scale=elevation_scale_val,
                                radius=4000,
                                get_fill_color='rgb',
                                pickable=True,
                                extruded = True,
                                auto_highlight=True)


        column_layer_map = pdk.Deck(layers=column_layer, 
                                initial_view_state=view, 
                                tooltip={
                                'html': '<b>Project Name:</b> {PRN}<br> <b>Client:</b> {Client}<br> <b>PVR:</b> {PVR}<br> <b>Duty Count:</b> {Opt2}<br> <b>Duties Saved:</b> {DCS}<br> <b>Paid Time Difference:</b> {PTDH}<br> <b>Efficiency Difference:</b> {EFD2}' ,
                                'style': {
                                    'color': 'white'
            },
            
            
        }, map_provider=map_provider,
    map_style=map_style)  
            
            # Adding code so we can have map default to the center of the data
        st.pydeck_chart(column_layer_map)

            
    with main_column_2: 
        with st.expander('**Pie Chart**', expanded = True):

            pie_dataframe_ba = pd.DataFrame(filtered_data, columns= ['Spread for BA', 'Attendance for BA',  'Driving time for BA' , 'Depot pull time for BA', 'Sign on time for BA', 'Sign off time for BA', 'Baseline Paid Time'])
            pie_dataframe_op = pd.DataFrame(filtered_data, columns=['Spread for OP','Attendance for OP', 'Driving time for OP', 'Depot pull time for OP', 'Sign on time for OP', 'Sign off time for OP', 'Optimisation Paid Time'])

            def create_new_column(df, string, cola, colb):
                df[string] = df[cola]-df[colb]
                return df

            def create_new_column_a(df, string, cola, colb):
                df[string] = df[cola]+df[colb]
                return df

            

            pie_dataframe_ba = create_new_column(pie_dataframe_ba, 'Time Unpaid BA', 'Spread for BA', 'Baseline Paid Time')
            pie_dataframe_op = create_new_column(pie_dataframe_op, 'Time Unpaid OP', 'Spread for OP', 'Optimisation Paid Time')
            pie_dataframe_ba = create_new_column(pie_dataframe_ba, 'Trip Time BA', 'Driving time for BA', 'Depot pull time for BA')
            pie_dataframe_op = create_new_column(pie_dataframe_op, 'Trip Time OP', 'Driving time for OP', 'Depot pull time for OP')
            pie_dataframe_ba = create_new_column_a(pie_dataframe_ba, 'Driver Signing BA', 'Sign on time for BA', 'Sign off time for BA')
            pie_dataframe_op = create_new_column_a(pie_dataframe_op, 'Driver Signing OP', 'Sign on time for OP', 'Sign off time for OP')

            pie_dataframe_ba = pie_dataframe_ba.drop(columns= ['Spread for BA', 'Baseline Paid Time', 'Driving time for BA', 'Sign on time for BA', 'Sign off time for BA'], axis=1)
            pie_dataframe_op = pie_dataframe_op.drop(columns= ['Spread for OP', 'Optimisation Paid Time', 'Driving time for OP', 'Sign on time for OP', 'Sign off time for OP'],axis=1)
            

            def create_sums_for_pie(df):
                list_sum = []
                for column in df:
                    list_sum.append(df[column].sum())
                return list_sum

            pie_sum_ba = create_sums_for_pie(pie_dataframe_ba)
            pie_sum_op = create_sums_for_pie(pie_dataframe_op)


            def create_percentages_for_pie(pie_sum):
                total = sum(pie_sum)
                percentages = [round(value / total * 100, 2) for value in pie_sum]
                return percentages

            pie_sum_perc_ba = create_percentages_for_pie(pie_sum_ba)
            pie_sum_perc_op = create_percentages_for_pie(pie_sum_op)

            def create_label_for_pie(df):
                df_columns = df.columns.to_list()
                return df_columns

            pie_ba_columns = create_label_for_pie(pie_dataframe_ba)
            pie_op_columns = create_label_for_pie(pie_dataframe_op)

            

           

            matplotlib.use("agg")
            _lock = RendererAgg.lock
            fig, ax = plt.subplots(figsize=(3, 3))
            ax.pie(pie_sum_ba, colors = [(0.1, 0.4, 0.5, 0.3),(0.1, 0.4, 0.5, 0.5),(0.1, 0.4, 0.5, 0.7),(0.1, 0.4, 0.5, 0.9),(0.1, 0.4, 0.5, 0.2)],  wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white'
            })
            #display a white circle in the middle of the pie chart
            p = plt.gcf()
            p.gca().add_artist(plt.Circle( (0,0), 0.7, color='white'))

            tabBa, tabOp  = st.tabs(['Baseline', 'Optimisation'])

            with tabBa:

                st.info('A Pie Chart to show the **Time** distribution across Duties for **Baseline** Schedules')
                col_pie_1, col_pie_2 = st.columns(2)
                col_pie_1.pyplot(fig)


                for i in range(3):
                    col_pie_2.write('')
                for i in range (len(pie_ba_columns)):
                    col_pie_2.write(f'**{pie_ba_columns[i]}**: {pie_sum_perc_ba[i]}%')
            with tabOp:
                st.info('A Pie Chart to show the **Time** distribution across Duties for **Optimised** Schedules')
                col_pie_1a, col_pie_2a = st.columns(2)
                fig1, ax1 = plt.subplots(figsize=(3, 3))
                ax1.pie(pie_sum_op, colors = [(0.1, 0.2, 0.5, 0.3),(0.1, 0.2, 0.5, 0.5),(0.1, 0.2, 0.5, 0.7),(0.1, 0.2, 0.5, 0.9),(0.1, 0.2, 0.5, 0.2)],   wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white'
                })
                #display a white circle in the middle of the pie chart
                p1 = plt.gcf()
                p1.gca().add_artist(plt.Circle( (0,0), 0.7, color='white'))
                col_pie_1a.pyplot(fig1)
                for i in range(3):
                    col_pie_2a.write('')
                for i in range (len(pie_op_columns)):
                    col_pie_2a.write(f'**{pie_op_columns[i]}**: {pie_sum_perc_op[i]}%')

            #get_json['stats']['crew_schedule_stats']['length'] - #get_json['stats']['crew_schedule_stats']['paid_time'] = time_unpaid
            #get_json['stats']['crew_schedule_stats']['attendance_time'] - attendance
            #get_json['stats']['crew_schedule_stats']['depot_pull_time'] - pull_Time
                        #get_json['stats']['crew_schedule_stats']['driving_time'] - get_json['stats']['crew_schedule_stats']['depot_pull_time'] = trip_time
                        #get_json['stats']['crew_schedule_stats']['sign_on_time']
                        #get_json['stats']['crew_schedule_stats']['sign_off_time']

           
            



        with st.expander('**Bar Chart**', expanded = True):
            corrlation_df_op  = pd.DataFrame(filtered_data, columns=['Efficiency Difference','Optimisation Duty Count'])
            corrlation_df_op_pvr  = pd.DataFrame(filtered_data, columns=['Efficiency Difference','PVR'])
            

            def cal_pearson_corr(correlation_df, col1, col2):
                mean_x = correlation_df[col1].mean()
                mean_y = correlation_df[col2].mean()
                std_x = correlation_df[col1].std()
                std_y = correlation_df[col2].std()

                corr_pearson = correlation_df[col1].cov(correlation_df[col2])/(std_x*std_y)
                corr_pearson = round(corr_pearson, 4)
                return corr_pearson

            corr_pearson_op = cal_pearson_corr(corrlation_df_op, 'Optimisation Duty Count', 'Efficiency Difference')
            corr_pearson_op_pvr = cal_pearson_corr(corrlation_df_op_pvr, 'PVR', 'Efficiency Difference')

            
            tab_pvr, tab_dc = st.tabs(['PVR','Duty Count'])
            with tab_pvr:
                st.info('This is a graph to show linear correlation between Efficiency Difference and PVR')
                fig = px.scatter(
                corrlation_df_op_pvr,
                x="PVR",
                y="Efficiency Difference",
                color="Efficiency Difference",
                color_continuous_scale="reds",
                trendline='ols',
                )
                #['lowess', 'rolling', 'ewm', 'expanding', 'ols']

                st.plotly_chart(fig, theme="streamlit", use_conatiner_width=True)

                with st.expander('**Description**'):
                    st.write(f'Pearson Correlation Coeff: **{corr_pearson_op_pvr}**')

            with tab_dc:
                st.info('This is a graph to show linear correlation between Efficiency Difference and Duty Count')
                fig = px.scatter(
                corrlation_df_op,
                x="Optimisation Duty Count",
                y="Efficiency Difference",
                color="Efficiency Difference",
                color_continuous_scale="reds",
                trendline='ols',
                )
                #['lowess', 'rolling', 'ewm', 'expanding', 'ols']

                st.plotly_chart(fig, theme="streamlit", use_conatiner_width=True)

                with st.expander('**Description**'):
                    st.write(f'Pearson Correlation Coeff: **{corr_pearson_op}**')

            





with tab1:
    #Submissions are password protected via a text input 
    password_record = st.text_input('Type password here', type="password")
    #TODO: Save this to a secrets.toml file 
    if password_record == 'abc123':
        #Expander info - first always expanded 
        with st.expander('**Post Schedule to Records**', expanded=True):
            #form create to submit record
            with st.form('API Requst parameters'):
            
                #Project Name text input as can't pull it consistently from API - MUST not be blank - validation step later on on form submission
                project_name = st.text_input('Name of Project', placeholder='Derby')
                #Text input for URL baseline 
                schedule_URL_baseline = st.text_input(label= 'Please type the baseline schedule URL here', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties')
                #Text input for URL optimisation 
                schedule_URL_optimisation = st.text_input(label= 'Please type the optimised schedule URL here', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties', key = 'b')
                
                #function to process URL into substring variables used for API 
                domain_name_ba, schedule_id_ba , project_id_ba = process_URL(schedule_URL_baseline)
                #//
                domain_name_op, schedule_id_op , project_id_op = process_URL(schedule_URL_optimisation)

                #Check if text input is not blank
                if schedule_URL_optimisation != '':
                    #Get id and secret based on url that has been entered 
                    client_id_baseline , client_secret_baseline= generate_auth(domain_name_ba, api_secrets_dict)
                    #//
                    client_id_optimisation, client_secret_optimisation= generate_auth(domain_name_op, api_secrets_dict)
                #Form submit button
                submit = st.form_submit_button('Submit')
                #IF clicked
                if submit:
                    #Check if project name is blank
                    if not project_name:
                        #Info 
                        st.warning("**Project Name** can't be left blank")
                    #check two conditions - firstly the project name does not match one in existing record (this is converted to lowercase for both so it is not case sensitive) - due to user input error
                    #                     - secondly: check project id does not match one from records
                    #check that project id matches for baseline and optimisation URL 
                    elif project_id_ba != project_id_op:
                        st.error('Please upload all comparisons from the **Same** project!')

                    #if all other conditions are met - continue to call the API 
                    else:
                        #Present progress bar 
                        my_bar = st.progress(0)
                        for percent_complete in range(100):
                            time.sleep(0.005)
                            my_bar.progress(percent_complete + 1)

                        ##    Function to obtain a new OAuth 2.0 token from the authentication server              
                        ## 	Obtain a token before calling the API for the first time
                        token_baseline = get_new_token(client_id_baseline, client_secret_baseline, domain_name_ba, 'Baseline')
                        token_optimisation = get_new_token(client_id_optimisation, client_secret_optimisation, domain_name_op, 'Optimisation')

                        #get_json_test1 = api_header_response(token_baseline, domain_name_ba, schedule_id_ba)
                        #st.write(get_json_test1)
                        #Example to get the optibus ID from a schedule and then use servrices endpoint
                        #.compensationtime
                        def get_optibus_id(token, domain_name, schedule_id):
                            api_call_headers = {'Authorization': 'Bearer ' + token}
                            api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedules/meta?scheduleIds[]={schedule_id}&includeHidden=true&includeDeleted=true', headers=api_call_headers, verify=False)
                            get_json = api_call_response.json()
                            for d in get_json:
                                optibus_id = d['schedule']['optibusId']
                                dataset_id = d['dataset']['optibusId']
                            return optibus_id, dataset_id
                        optibus_id_ba, dataset_id_ba = get_optibus_id(token_baseline, domain_name_ba, schedule_id_ba)
                        optibus_id_op, dataset_id_op = get_optibus_id(token_optimisation, domain_name_op, schedule_id_op)

                        
                        

                        if [d for d in data if d['Dataset ID'] == dataset_id_ba]:
                        #validation info
                            st.warning(f'The schedule **{project_name}** already exists in records based on sharing a common dataset_id , please submit a different project')
                            st.stop()
                        

                        if 'status' in optibus_id_ba and optibus_id_ba['status'] == 500:
                            url_check = 'Baseline URL'
                            st.warning(f'There is an issue with **{url_check}**, please *Save a new version of the schedule* and try again, this is a known API issue. Please see message below for further details')
                            st.caption(optibus_id_ba)
                            st.stop()
                        elif 'status' in optibus_id_op and optibus_id_op['status'] == 500:
                            url_check = 'Optimisation URL'
                            st.warning(f'There is an issue with **{url_check}**, please *Save a new version of the schedule* and try again, this is a known API issue. Please see message below for further details')
                            st.caption(optibus_id_op)
                            st.stop()

                        get_services_json_ba = api_services_response(token_baseline, domain_name_ba, optibus_id_ba)
                        get_services_json_op = api_services_response(token_optimisation, domain_name_op, optibus_id_op)
                        

                        #&statProperties[]=crew_schedule_stats.paid_time&statProperties[]=general_stats&statProperties[]=relief_vehicle_schedule_stats&statProperties[]=relief_vehicle_schedule_stats
                        #st.write(get_services_json_ba)
                        #st.write(get_services_json_ba)

                        def api_meta_response(token, domain_name, schedule_id):
                            api_call_headers = {'Authorization': 'Bearer ' + token}

                            #Stat property list
                            stat_properties = ["crew_schedule_stats.paid_time", 
                            "crew_schedule_stats.attendance_time", 
                            "crew_schedule_stats.custom_time_definitions", 
                            "crew_schedule_stats.depot_pull_time", 
                            "crew_schedule_stats.duties_count", 
                            "crew_schedule_stats.histograms", 
                            "crew_schedule_stats.length", 
                            "crew_schedule_stats.sign_off_time", 
                            "crew_schedule_stats.sign_on_time", 
                            "crew_schedule_stats.split_count", 
                            "vehicle_schedule_stats.depot_allocations", 
                            "vehicle_schedule_stats.driving_time", 
                            "vehicle_schedule_stats.platform_time", 
                            "vehicle_schedule_stats.pvr"]

                            #Initial call without parameters
                            api_call = f'https://{domain_name}.optibus.co/api/v2/schedules/meta?scheduleIds[]={schedule_id}&includeHidden=true&includeDeleted=true'

                            #Iterate and append parameters to the stat_property component of api string
                            for property in stat_properties:
                                api_call += f'&statProperties[]={property}'

                            api_call_response = requests.get(api_call, headers=api_call_headers, verify=False)

                            #OLD API STRING (QUITE DIFFICULT TO READ)
                            #api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedules/meta?scheduleIds[]={schedule_id}&includeHidden=true&includeDeleted=true&statProperties[]=crew_schedule_stats.paid_time&statProperties[]=crew_schedule_stats.attendance_time&statProperties[]=crew_schedule_stats.custom_time_definitions&statProperties[]=crew_schedule_stats.depot_pull_time&statProperties[]=crew_schedule_stats.duties_count&statProperties[]=crew_schedule_stats.histograms&statProperties[]=crew_schedule_stats.length&statProperties[]=crew_schedule_stats.sign_off_time&statProperties[]=crew_schedule_stats.sign_on_time&statProperties[]=crew_schedule_stats.split_count&statProperties[]=vehicle_schedule_stats.depot_allocations&statProperties[]=vehicle_schedule_stats.driving_time&statProperties[]=vehicle_schedule_stats.platform_time&statProperties[]=vehicle_schedule_stats.pvr', headers=api_call_headers, verify=False)

                            get_json = api_call_response.json()
                            return get_json
                        

                        def create_json_list(get_services_json, token, domain_name):
                            emp_list = []
                            exclude = ['NWD', '#SCH', 'NSCH']
                            for d in get_services_json:
                                if not any(substring in d['name'] for substring in exclude):
                                    emp_list.append(api_meta_response(token, domain_name, d['id']))

                            flattened_list = [item for sublist in emp_list for item in sublist]
                            return flattened_list

                        json_data_list_ba = create_json_list(get_services_json_ba, token_baseline, domain_name_ba)
                        json_data_list_op = create_json_list(get_services_json_op, token_optimisation, domain_name_op)

                       
                        

                        ##   Call the API with the token
                        #get_json_ba = api_header_response(token_baseline, domain_name_ba, schedule_id_ba)
                        #get_json_op = api_header_response(token_optimisation, domain_name_op, schedule_id_op)

                        
                        
                        
                       
                        

                        

                        
                        
                            # do something with d2
                        #assign client_instance from identifying substring in key of dictionary
                        for key in clients_dict:
                            # check if the key is a substring of the string
                            if key in domain_name_ba:
                                # if it is, assign a new variable the corresponding value
                                client_instance = clients_dict[key]

                        #call functions defined earlier on to get specific data from the API - CAN ALWAYS BE UPDATED and points added 
                        #dow_ba = get_days_of_week(get_json_ba)
                        #dow_op = get_days_of_week(get_json_op)
                        #opId_ba = get_optibus_id(get_json_ba)
                        #opId_op = get_optibus_id(get_json_op)

                        

                        #st.write(json_data_list_ba)
                        



                        #json_data_list_ba = create_json_list(get_services_json_ba, token_baseline, domain_name_ba)
                        #json_data_list_op = create_json_list(get_services_json_op, token_optimisation, domain_name_op)


                        
                        #BASELINE : https://arriva-uk-bus-northwest.optibus.co/project/da336nrgv/schedules/EvltiNwWMS/gantt?type=duties
                        #OPTIMISATION: https://arriva-uk-bus-northwest.optibus.co/project/da336nrgv/schedules/bBIr4mZwjT/gantt?type=duties
                        #Baseline is inserting two service groups and optimisation inserting 3, so problematic as calculations are wrong, count number of list elements to match to mitigate this 
                       

                        def catch_service_lists(json_data_list, key, key2):
                            result = []
                            for d in json_data_list:
                                result.extend(d.get(key, {}).get(key2, []))
                            return result

                        check_serv_ba = catch_service_lists(json_data_list_ba, 'service', 'daysOfWeek')
                        check_serv_op = catch_service_lists(json_data_list_op, 'service', 'daysOfWeek')

                        def return_assciated_Serv_days(check_serv, string):
                            master_list = [2,3,4,5,6,7,1]
                            master_dict =  service_days_dict={1:'Sun',2:'Mon',3:'Tue',4:'Wed',5:'Thur',6:'Fri',7:'Sat'}
                            missing_elements = set(master_list) - set(check_serv)
                            missing_days = [master_dict[x] for x in missing_elements] 
                            return missing_days, string

                        missing_days_ba, identifier_ba = return_assciated_Serv_days(check_serv_ba, 'Baseline')
                        missing_days_op, identifier_op = return_assciated_Serv_days(check_serv_ba, 'Optimisation')


                        if len(missing_days_ba) != 0:
                            st.error(f"API Error Occuring for **{missing_days_ba}** on **{identifier_ba}** schedule for **{project_name}**")
                            st.stop()
                        elif len(missing_days_ba) != 0:
                            st.error(f"API Error Occuring for **{missing_days_op}** on **{identifier_op}** schedule for **{project_name}**")
                            st.stop()

                       

                        
                        #CONDITION THAT CHECKS - THIS SHOULD WORK AS FUNCTION ABOVE DROPS DICTS CONTAINING THE ERROR 500 

                        #sch_d['service']['daysOfWeek']

                        if len(json_data_list_ba)!= len(json_data_list_op):
                            st.error('Number of Service Days do not match between benchmark and optimisation, this could be that one of the api requests is erroring on a specific day')
                            st.stop()

                        

                        
                        

                        paid_time_list_ba, paid_time_list_sum_ba = create_paid_time_list(json_data_list_ba)
                        paid_time_list_op, paid_time_list_sum_op = create_paid_time_list(json_data_list_op)

                        

                        #TODO: Get split counts and sum for all days
                        #TODO: GET paid break counts - may have to iterate through twice



                        split_count_list_ba, split_count_list_sum_ba = create_split_count_list(json_data_list_ba)
                        split_count_list_op, split_count_list_sum_op = create_split_count_list(json_data_list_op)
                        




                        paid_break_sum_ba = create_paid_break_time_list(json_data_list_ba)
                        paid_break_sum_op = create_paid_break_time_list(json_data_list_op)
                       
                       
                        

                        #values = [inner_dict["value"] for outer_dict in json_data_list for inner_dict in outer_dict['stats']['crew_schedule_stats']['custom_time_definitions'] if inner_dict["name"] == 'Paid Break']


                        
                        

                       

                        platform_time_list_ba, platform_time_list_sum_ba = create_platform_time_list(json_data_list_ba)
                        platform_time_list_op, platform_time_list_sum_op = create_platform_time_list(json_data_list_op)


                        duty_count_list_ba, duty_count_list_sum_ba = create_duty_count_list(json_data_list_ba)
                        duty_count_list_op, duty_count_list_sum_op = create_duty_count_list(json_data_list_op)

                                
                        avg_paid_time_ba = calculate_avg_paid_time(paid_time_list_sum_ba,duty_count_list_sum_ba)
                        avg_paid_time_op = calculate_avg_paid_time(paid_time_list_sum_op,duty_count_list_sum_op)
                        
                        efficiency_ba = get_sch_eff(platform_time_list_sum_ba, paid_time_list_sum_ba)
                        efficiency_op = get_sch_eff(platform_time_list_sum_op, paid_time_list_sum_op)
                        eff_diff = calculate_eff_diff(efficiency_ba, efficiency_op)
                        duty_count_diff = calculate_duty_diff(duty_count_list_sum_ba, duty_count_list_sum_op)
                        pt_diff = calculate_paid_time_diff(paid_time_list_sum_ba, paid_time_list_sum_op)

                        

                        

                        #Need to consider other regions

                      
                

                       


                        spread_list_sum_ba = create_generic_time_stat_list(json_data_list_ba, 'length', 'Baseline','crew_schedule_stats')
                        spread_list_sum_op = create_generic_time_stat_list(json_data_list_op, 'length', 'Optimisation','crew_schedule_stats')

                        attendance_time_list_sum_ba = create_generic_time_stat_list(json_data_list_ba, 'attendance_time', 'Baseline','crew_schedule_stats')
                        attendance_time_list_sum_op = create_generic_time_stat_list(json_data_list_op, 'attendance_time', 'Optimisation','crew_schedule_stats')



                        driving_time_list_sum_ba = create_generic_time_stat_list(json_data_list_ba, 'driving_time', 'Baseline','vehicle_schedule_stats')
                        driving_time_list_sum_op = create_generic_time_stat_list(json_data_list_op, 'driving_time', 'Optimisation','vehicle_schedule_stats')

                        depot_pull_time_list_sum_ba = create_generic_time_stat_list(json_data_list_ba, 'depot_pull_time', 'Baseline','crew_schedule_stats')
                        depot_pull_time_list_sum_op = create_generic_time_stat_list(json_data_list_op, 'depot_pull_time', 'Optimisation','crew_schedule_stats')

                        sign_on_time_list_sum_ba = create_generic_time_stat_list(json_data_list_ba, 'sign_on_time', 'Baseline','crew_schedule_stats')
                        sign_on_time_list_sum_op = create_generic_time_stat_list(json_data_list_op, 'sign_on_time', 'Optimisation','crew_schedule_stats')

                        sign_off_time_list_sum_ba = create_generic_time_stat_list(json_data_list_ba, 'sign_off_time', 'Baseline','crew_schedule_stats')
                        sign_off_time_list_sum_op = create_generic_time_stat_list(json_data_list_op, 'sign_off_time', 'Optimisation','crew_schedule_stats')



                        def get_pvr(json_data_list, string, key_val):

                            try:
                            # List to store the results
                                result = max(d['service']['stats'][key_val][string] for d in json_data_list)
                            except KeyError:
                                result = 0
                                st.warning(f'We have had to assume this {string} is 0 due to key not being found')

                            return  result

                        pvr_max = get_pvr(json_data_list_op, 'pvr', 'vehicle_schedule_stats')

                        schedule_id_list = [item['scheduleId'] for item in json_data_list_ba]

                        

                        def api_header_response_tp(token, domain_name, schedule_id_list):
                            api_call_headers = {'Authorization': 'Bearer ' + token}
                            for i in range(len(schedule_id_list)):
                                api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedule/{schedule_id_list[i]}?needStats=true', headers=api_call_headers, verify=False)
                                get_json = api_call_response.json()
                                if 'status' not in get_json:
                                    return get_json
                                    break
                            return None
                        
                        get_json_tp = api_header_response_tp(token_baseline, domain_name_ba, schedule_id_list)

                        depot_item_ba = get_depot_from_api(get_json_tp)

                        dict_depot, stop_name ,lat, long = get_stop_details_from_depot_id(get_json_tp, depot_item_ba)

                        
            


                                                # initialize Nominatim API

                        def get_county_from_coords(lat, long):
                            geolocator = Nominatim(user_agent="geoapiExercises")
                            # Latitude & Longitude input
                            Latitude = str(lat)
                            Longitude = str(long)
                            location = geolocator.reverse(Latitude+","+Longitude)
                            address = location.raw['address']
                            # traverse the data
                        
                            country = address.get('country', '')
                            return country

                        country = get_county_from_coords(lat, long)

                        

                        

                        region = get_region_from_country(emea_str, 'EMEA', country)

                        

                        
                        

                        #TODO: Implement paid break totals into these 
                        # Query api for other stats we can leverage to present to the user

                        
                        #Get the index you wish to post the data to for the gsheet
                        get_post_row_index = len(data)+2

                        #entry for a row has to be inserted as a list,
                        # ALL data into sheet has not been formatted, that happens when querying the data as easier to transform at endpoint
                        insert_entry = [project_name, project_id_ba, schedule_URL_baseline, 
                        duty_count_list_sum_ba, paid_time_list_sum_ba, avg_paid_time_ba, 
                        platform_time_list_sum_ba, schedule_URL_optimisation, duty_count_list_sum_op, paid_time_list_sum_op, 
                        avg_paid_time_op, platform_time_list_sum_op, eff_diff, duty_count_diff, pt_diff,  domain_name_ba, client_instance, optibus_id_ba, paid_break_sum_ba, paid_break_sum_op, split_count_list_sum_ba, split_count_list_sum_op, stop_name, lat, long, country, region, spread_list_sum_ba, spread_list_sum_op,
attendance_time_list_sum_ba,
attendance_time_list_sum_op,
driving_time_list_sum_ba,
driving_time_list_sum_op,
depot_pull_time_list_sum_ba,
depot_pull_time_list_sum_op,
sign_on_time_list_sum_ba,
sign_on_time_list_sum_op,
sign_off_time_list_sum_ba,
sign_off_time_list_sum_op, 
dataset_id_ba, 
pvr_max
                        ]

                        #Info 
                        st.success(f'Inserted the following record: **{project_name}**')

                        #Create a record to present to the user of what has been submitted to the gsheet (almost like a reciept)
                        present_dict = {
                            'Project Name': [project_name], 
                            'Project ID': [project_id_ba], 
                            'Baseline URL':[schedule_URL_baseline], 
                            'Baseline Duty Count': [duty_count_list_sum_ba], 
                            'Baseline Paid Time': [paid_time_list_sum_ba], 
                            'Baseline Av. Paid Time': [avg_paid_time_ba], 
                            'Baseline Platform Time': [platform_time_list_sum_ba],
                            'Optimisation URL': [schedule_URL_optimisation],
                            'Optimisation Duty Count': [duty_count_list_sum_op], 
                            'Optimisation Paid Time': [paid_time_list_sum_op],
                            'Optimisation Av. Paid Time': [avg_paid_time_op], 
                            'Optimisation Platform Time': [platform_time_list_sum_op], 
                            'Efficiency Difference': [eff_diff], 
                            'Duty Couunt Difference': [duty_count_diff],
                            'Paid Time Difference': [pt_diff],
                            'Domain': [domain_name_ba],
                            'Client': [client_instance],
                            'OptibusId': [optibus_id_ba], 
                            'Baseline Paid Break':[paid_break_sum_ba], 
                            'Optimisation Paid Break':[paid_break_sum_op], 
                            'Baseline Split Count': [split_count_list_sum_ba], 
                            'Optimisation Split Count': [split_count_list_sum_op],
                            'Depot Name': [stop_name], 
                            'Latitude':[lat], 
                            'Longitude':[long], 
                            'Country': [country], 
                            'Region': [region], 
                            'Spread for BA':[spread_list_sum_ba], 
                            'Spread for OP':[spread_list_sum_op], 
                            'Attendance for BA':[attendance_time_list_sum_ba], 
                            'Attendance for OP':[attendance_time_list_sum_op]
                            #TODO: present rest of variables
                        }
                        #Present this as a dataframe (easy to read)
                        st.dataframe(pd.DataFrame(data = present_dict), use_container_width=True)

                        #Insert the row into the googlesheet 
                        sheet.insert_row(insert_entry, get_post_row_index)
                       

        

        with st.expander('**Update Schedule From Record**', expanded=False):

            project_name_list, proj_concat_list = get_values(data, 'Project Name', 'Client')
            #Insert a default option of none into the select box list so doesn't rerun everytime 
            proj_concat_list.insert(0, 'None')
            #Create the selectbox with the concat list
            select_record = st.selectbox('Select your record you wish to update', proj_concat_list)
            #Check if the value in the select is not none
            if select_record != 'None':
                #Get the index of the selected concatenated list element
                index_select = proj_concat_list.index(select_record)
                #return the target project name of using index -1 of the concat list (-1 due to inserting the 'None' string at start of list)
                target_project = project_name_list[index_select-1]
                #return the dictionary where the value is equal to target project using the key project name
                target_dict = [d for d in data if d['Project Name'] == target_project]
                #This target dict is currently wrapped in a list due to list of dictionaries return from whole data sample 
                #Empty dict to store target dict in
                result = {}
                for d in target_dict:
                    #Result is the single returned dictionary result
                    result.update(d)
                #write this dictionary record into a dataframe with index set to 0 as will only ever return a single record
                st.write(pd.DataFrame(result, index= [0]))
                #store the project name value into a single variable, this variable is immutable for the update record
                project_name = result['Project Name']

                #identify the index of the located dictionary in the list of dictionarys using this function, this will help locate the correct row in the sheet for updating it
                def get_index(dict_list, key, value):
                    for i, d in enumerate(dict_list):
                        if d[key] == value:
                            return i
                    return -1

                #the function will use the value of the project name to locate the index and then add 2 to this due to index starting at 2 in gsheet
                update_sheet_index = get_index(data, 'Project Name', target_project)+2
                #Checkbox boolean used to retain baseline url from old record and hide text input for baseline url
                retain_baseline_record = st.checkbox('Retain Baseline Details in Record')
                
                
                #if record is selected (not none), a form is presented to paste urls of 'Updated schedules
                with st.form('Input New Params for Update'):
                    #Info 
                    st.write('Paste in your URLS that you wish to overwrite the API Data with')
                    #Info 
                    st.caption('*Please Note the project Name will stay the Same*')
                    
                    #Boolean to check if condition is met 
                    if retain_baseline_record:
                        #Set variable to value of baseline URL key in result dict
                        schedule_URL_baseline = result['Baseline URL']
                        #Render this URL that is being used to the user 
                        st.info(f'Using the following URL baseline *{schedule_URL_baseline}*')
                        pass
                    else:
                        #If condition is not met (checkbox is false), text input is rendered for baseline url 
                        schedule_URL_baseline = st.text_input(label= 'Please type the baseline schedule URL here', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties', key='c')
                    
                    #Optimisation URL text input rendered by default 
                    schedule_URL_optimisation = st.text_input(label= 'Please type the optimised schedule URL here', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties', key = 'd')

                    #function to split individual coponents of URL into function substrings for API and other data purposes
                    domain_name_ba, schedule_id_ba , project_id_ba = process_URL(schedule_URL_baseline)
                    domain_name_op, schedule_id_op , project_id_op = process_URL(schedule_URL_optimisation)
                    #Submit button for form
                    submit = st.form_submit_button('Submit')
                    #If true
                    if submit:
                        #SAME API LOGIC AS in first expander (see line ~203 for comments)
                        if schedule_URL_optimisation != '':

                            client_id_baseline , client_secret_baseline= generate_auth(domain_name_ba, api_secrets_dict)
                            client_id_optimisation, client_secret_optimisation= generate_auth(domain_name_op, api_secrets_dict)
                            if project_id_ba != project_id_op:
                                st.error('Please upload all comparisons from the **same** project!')
                                
                            else:
                                my_bar = st.progress(0)
                                for percent_complete in range(100):
                                    time.sleep(0.01)
                                    my_bar.progress(percent_complete + 1)

                                token_baseline = get_new_token(client_id_baseline, client_secret_baseline, domain_name_ba, 'Baseline')
                                token_optimisation = get_new_token(client_id_optimisation, client_secret_optimisation, domain_name_op, 'Optimisation')

                                get_json_ba = api_header_response(token_baseline, domain_name_ba, schedule_id_ba)
                                get_json_op = api_header_response(token_optimisation, domain_name_op, schedule_id_op)

                                for key in clients_dict:
                                    # check if the key is a substring of the string
                                    if key in domain_name_ba:
                                        # if it is, assign a new variable the corresponding value
                                        client_instance = clients_dict[key]

                                dow_ba = get_days_of_week(get_json_ba)
                                dow_op = get_days_of_week(get_json_op)
                                opId_ba = get_optibus_id(get_json_ba)
                                opId_op = get_optibus_id(get_json_op)
                                get_services_json_ba = api_services_response(token_baseline, domain_name_ba, opId_ba)
                                get_services_json_op = api_services_response(token_optimisation, domain_name_op, opId_op)

                                json_data_list_ba = create_json_list(get_services_json_ba, token_baseline, domain_name_ba)
                                json_data_list_op = create_json_list(get_services_json_op, token_optimisation, domain_name_op)

                                paid_time_list_ba, paid_time_list_sum_ba = create_paid_time_list(json_data_list_ba)
                                paid_time_list_op, paid_time_list_sum_op = create_paid_time_list(json_data_list_op)

                                split_count_list_ba, split_count_list_sum_ba = create_split_count_list(json_data_list_ba)
                                split_count_list_op, split_count_list_sum_op = create_split_count_list(json_data_list_op)

                                paid_break_sum_ba = create_paid_break_time_list(json_data_list_ba)
                                paid_break_sum_op = create_paid_break_time_list(json_data_list_op)

                                platform_time_list_ba, platform_time_list_sum_ba = create_platform_time_list(json_data_list_ba)
                                platform_time_list_op, platform_time_list_sum_op = create_platform_time_list(json_data_list_op)

                                duty_count_list_ba, duty_count_list_sum_ba = create_duty_count_list(json_data_list_ba)
                                duty_count_list_op, duty_count_list_sum_op = create_duty_count_list(json_data_list_op)

                                
                                avg_paid_time_ba = calculate_avg_paid_time(paid_time_list_sum_ba,duty_count_list_sum_ba)
                                avg_paid_time_op = calculate_avg_paid_time(paid_time_list_sum_op,duty_count_list_sum_op)
                                
                                efficiency_ba = get_sch_eff(platform_time_list_sum_ba, paid_time_list_sum_ba)
                                efficiency_op = get_sch_eff(platform_time_list_sum_op, paid_time_list_sum_op)
                                eff_diff = calculate_eff_diff(efficiency_ba, efficiency_op)
                                duty_count_diff = calculate_duty_diff(duty_count_list_sum_ba, duty_count_list_sum_op)
                                pt_diff = calculate_paid_time_diff(paid_time_list_sum_ba, paid_time_list_sum_op)

                                depot_item_ba = get_depot_from_api(get_json_ba)

                                dict_depot, stop_name ,lat, long = get_stop_details_from_depot_id(get_json_ba, depot_item_ba)

                            ######################################################################################
                                
                            #update entry as list with new variables

                                update_entry = [project_name, project_id_ba, schedule_URL_baseline, 
                        duty_count_list_sum_ba, paid_time_list_sum_ba, avg_paid_time_ba, 
                        platform_time_list_sum_ba, schedule_URL_optimisation, duty_count_list_sum_op, paid_time_list_sum_op, 
                        avg_paid_time_op, platform_time_list_sum_op, eff_diff, duty_count_diff, pt_diff,  domain_name_ba, client_instance, opId_ba, paid_break_sum_ba, paid_break_sum_op, split_count_list_sum_ba, split_count_list_sum_op, stop_name, lat, long
                        ]

                                #As you can't batch update the row, had to use a for loop for the number of items in the update entry list and iterate through each cell of the row
                                for i in range(0, len(update_entry)):
                                    sheet.update_cell(update_sheet_index, i+1, update_entry[i])

                                #Info
                                st.success(f'Updated the following record: **{project_name}**')
                                present_dict = {
                                    'Project Name': [project_name], 
                                    'Project ID': [project_id_ba], 
                                    'Baseline URL':[schedule_URL_baseline], 
                                    'Baseline Duty Count': [duty_count_list_sum_ba], 
                                    'Baseline Paid Time': [paid_time_list_sum_ba], 
                                    'Baseline Av. Paid Time': [avg_paid_time_ba], 
                                    'Baseline Platform Time': [platform_time_list_sum_ba],
                                    'Optimisation URL': [schedule_URL_optimisation],
                                    'Optimisation Duty Count': [duty_count_list_sum_op], 
                                    'Optimisation Paid Time': [paid_time_list_sum_op],
                                    'Optimisation Av. Paid Time': [avg_paid_time_op], 
                                    'Optimisation Platform Time': [platform_time_list_sum_op], 
                                    'Efficiency Difference': [eff_diff], 
                                    'Duty Couunt Difference': [duty_count_diff],
                                    'Paid Time Difference': [pt_diff],
                                    'Domain': [domain_name_ba],
                                    'Client': [client_instance],
                                    'OptibusId': [opId_ba],
                                    'Baseline Paid Break':[paid_break_sum_ba], 
                                    'Optimisation Paid Break':[paid_break_sum_op], 
                                    'Baseline Split Count': [split_count_list_sum_ba], 
                                    'Optimisation Split Count': [split_count_list_sum_op], 
                                    'Depot Name': [stop_name], 
                                    'Latitude':[lat], 
                                    'Longitude':[long]
                                }
                                #present updated record
                                st.dataframe(pd.DataFrame(data = present_dict), use_container_width=True)

            #If none (default is selected) prompt user
            else:
                st.info('Please Select a project you wish to update')
                    
    
        #becuase logic of creating record variables for update are embedded on a condition of not none in select box, have to redefine these variables in this section
        with st.expander('**Delete Schedule From Record**', expanded=False):
            #LOGIC Same as update record (see line ~ 313)
            def get_values(dict_list, key1, key2):
                return [d[key1] for d in dict_list], [d[key1] +' - '+ d[key2] for d in dict_list]
            project_name_list, proj_concat_list = get_values(data, 'Project Name', 'Client')
            proj_concat_list.insert(0, 'None')
            select_record = st.selectbox('Select your record you wish to update', proj_concat_list, key='e')
            if select_record != 'None':
                index_select = proj_concat_list.index(select_record)
                target_project = project_name_list[index_select-1]
                    
                target_dict = [d for d in data if d['Project Name'] == target_project]
                result = {}
                for d in target_dict:
                    result.update(d)

                ############################################################################    

                st.write(pd.DataFrame(result, index= [0]))
                #display button to delete record
                delete_record = st.button('Delete Record', type='primary' )

                #get index for where to post record to archives gsheet
                get_post_row_index_archive = len(data_archives)+2

                #if button is clicked
                if delete_record:
                    

                    #create a list for the values of the selected result to post to archives
                    archive_record = [value for value in result.values()]

                    #get the index using function defined above to delete the record
                    delete_record_sheet_index = get_index(data, 'Project Name', target_project)+2

                    #Post record to archive sheet
                    sheet_archive.insert_row(archive_record, get_post_row_index_archive)
                    #Present success for deletion

                    #TODO: returning project name doesn't work, need to re pull it from dict as not defined in method above
                    st.success(f'**{target_project}** was deleted from records')
                    #Delete row from master sheet
                    sheet.delete_row(delete_record_sheet_index)
                    
                    
                    #Rerun script to update data in dropdowns to reflect a record was deleted (otherwise won't rerun)
                    st.experimental_rerun()

        with st.expander('**Debug Schedule Record**', expanded=False):
            with st.form('API Reqeust parameters 2'):
                schedule_URL_dbug = st.text_input(label= 'Please type the schedule URL you would like to debug', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties', key = 'p')
                domain_name_dbug, schedule_id_dbug , project_id_dbug = process_URL(schedule_URL_dbug)
                if schedule_URL_dbug != '':
                    client_id_dbug, client_secret_dbug= generate_auth(domain_name_dbug, api_secrets_dict)
                submit = st.form_submit_button('Submit')
                if submit:
                    my_bar = st.progress(0)
                    for percent_complete in range(100):
                        time.sleep(0.01)
                        my_bar.progress(percent_complete + 1)
                if schedule_URL_dbug != '':
                    token_dbug = get_new_token(client_id_dbug, client_secret_dbug, domain_name_dbug, 'Optimisation')
                    get_json_dbug = api_header_response(token_dbug, domain_name_dbug, schedule_id_dbug)

                    st.write(get_json_dbug)
                    st.write(get_json_dbug['stats']['crew_schedule_stats']['paid_time'])


    #check if password is blank
    elif not password_record:
        st.warning('password cannot be blank')

    #check if password is incorrect
    else: 
        st.warning('Please enter Correct Password')


#Section to query data and create a custom report
with tab2:
    st.subheader('Query Records')

    client_set = list(set([d["Client"] for d in data]))
    client_set.insert(0, 'All')

    selected_clients = st.multiselect("Select Client(s):", client_set)

    try:
    
        if selected_clients == ['All']:
            selected_subdomains = None
            selected_domain_data = data

        elif selected_clients == []: 
            st.warning('Please Select Client')
            
        elif selected_clients != []:
            selected_clients_data = [d for d in data if d["Client"] in selected_clients]

            subdomain_set = list(set([d["Domain"] for d in selected_clients_data]))
            subdomain_set.insert(0, 'All')
            selected_subdomains = st.multiselect("Select Subdomains", subdomain_set)

            

            if selected_subdomains == ['All']:
                selected_domain_data = selected_clients_data
            elif selected_subdomains == []:
                pass
            else:
                selected_domain_data = [d for d in data if d["Domain"] in selected_subdomains]
    finally:

        if selected_clients == []:
            st.stop()
        else:
            if selected_subdomains == []:
                st.stop()
            else:


                domain_dataframe = pd.DataFrame(selected_domain_data)
                with st.form('Customise your Dataframe'):
                    st.write('**Customise your data export**')
                    

                    def format_list_string(value):
                        title_string = ' '.join(value)
                        title_string = title_string.strip("'")
                        return title_string

                    if selected_subdomains != None:
                        val = f'{format_list_string(selected_clients)} - {format_list_string(selected_subdomains)}'
                    else: 
                        val = 'All Clients'

                    file_title = st.text_input(label= 'Insert your file title', value = val)
                    st.markdown('---')
                    st.write('*What to include in your dataframe*?')
                    
                    st.caption('*Select the options you wish to include*')
                    
                    col1, col2 = st.columns(2)
                    
                    project_name_check = col1.checkbox('Project Name', value=True, disabled=True)
                    
                    client_check = col2.checkbox('Client', value=False)
                    domain_check = col1.checkbox('Domain', value = False)
                    efficiency_diff_check = col2.checkbox('Efficiency Difference', value=True)
                    duty_count_diff_check = col1.checkbox('Duty Count Difference', value=True)
                    paid_time_diff_check = col2.checkbox('Paid Time Difference', value=True)
                    st.write('')
                    with st.expander('⚙️ **Advanced Options**'):
                        col7, col8 = st.columns(2)
                        baseline_url_check = col7.checkbox('Baseline Url', value=False)
                        baseline_duty_count_check = col7.checkbox('Baseline Duty Count', value=False)
                        baseline_paid_time_check = col7.checkbox('Baseline Paid Time', value=True, disabled=True)
                        baseline_AVG_paid_time_check = col7.checkbox('Baseline AVG Paid Time', value=False)
                        baseline_platform_time_check = col7.checkbox('Baseline Platform Time', value=True, disabled=True)
                        baseline_paid_break_time_check = col7.checkbox('Baseline Paid Break Time', value=False)
                        baseline_split_count_check = col7.checkbox('Baseline Split Count', value=False)

                        optimisation_url_check = col8.checkbox('Optimisation Url', value=False)
                        optimisation_duty_count_check = col8.checkbox('Optimisation Duty Count', value=False)
                        optimisation_paid_time_check = col8.checkbox('Optimisation Paid Time', value=True, disabled=True)
                        optimisation_AVG_paid_time_check = col8.checkbox('Optimisation AVG Paid Time', value=False)
                        optimisation_platform_time_check = col8.checkbox('Optimisation Platform Time', value=True, disabled=True)
                        optimisation_paid_break_time_check = col8.checkbox('Optimisation Paid Break Time', value=False)
                        optimisation_split_count_check = col8.checkbox('Optimisation Split Count', value=False)

                    st.write('---')
                    st.caption('*File Format*')
                    file_type = st.radio('file format', ('XLSX', 'CSV', 'TXT', 'Preview Only', 'KPIs'), horizontal=True, label_visibility='collapsed')
                    st.markdown('---')

                    
                    
                    submit_custom = st.form_submit_button('Submit')

                if submit_custom:
                    if file_title != '':
                        checkbox_vars = []

                        def check_assignment(var, string, empt_list):
                            if var == True:
                                var = string
                                return empt_list.append(var)

                        check_assignment(project_name_check, 'Project Name', checkbox_vars)
                        check_assignment(domain_check, 'Domain', checkbox_vars)
                        check_assignment(client_check, 'Client', checkbox_vars)
                        check_assignment(efficiency_diff_check, 'Efficiency Difference', checkbox_vars)
                        check_assignment(duty_count_diff_check,'Duty Count Difference', checkbox_vars)
                        check_assignment(paid_time_diff_check,'Paid Time Difference', checkbox_vars)
                        check_assignment(baseline_url_check,'Baseline URL', checkbox_vars)
                        check_assignment(baseline_duty_count_check,'Baseline Duty Count', checkbox_vars)
                        check_assignment(baseline_paid_time_check,'Baseline Paid Time', checkbox_vars)
                        check_assignment(baseline_AVG_paid_time_check,'Baseline Av. Paid Time', checkbox_vars)
                        check_assignment(baseline_platform_time_check,'Baseline Platform Time', checkbox_vars)
                        check_assignment(baseline_paid_break_time_check, 'Baseline Paid Break Time', checkbox_vars)
                        check_assignment(baseline_split_count_check, 'Baseline Split Count', checkbox_vars)
                        check_assignment(optimisation_url_check,'Optimisation URL', checkbox_vars)
                        check_assignment(optimisation_duty_count_check,'Optimisation Duty Count', checkbox_vars)
                        check_assignment(optimisation_paid_time_check,'Optimisation Paid Time', checkbox_vars)
                        check_assignment(optimisation_AVG_paid_time_check,'Optimisation Av. Paid Time', checkbox_vars)
                        check_assignment(optimisation_platform_time_check,'Optimisation Platform Time', checkbox_vars)
                        check_assignment(optimisation_paid_break_time_check, 'Optimisation Paid Break Time', checkbox_vars)
                        check_assignment(optimisation_split_count_check, 'Optimisation Split Count', checkbox_vars)
                        

                        domain_dataframe = domain_dataframe[checkbox_vars]
                                        
                        numeric_columns = domain_dataframe.select_dtypes(include=['int', 'float'])

            
                        total_row = pd.DataFrame(index=["total"])
                        average_row = pd.DataFrame(index=["average"])
                        
                        

                        for col in numeric_columns.columns:
                            # Calculate the sum of the values in the column
                            col_sum = numeric_columns[col].sum()
                            avg_sum = numeric_columns[col].mean()

                           
                            
                            # Set the value of the "total" row for the current column
                            total_row.loc["total", col] = col_sum
                            average_row.loc['average', col] = avg_sum

                        def replace_column_value(df, column_name,baseline_platform_time,baseline_paid_time,opt_platform_time,opt_paid_time ):
                        # Check if the column exists in the DataFrame
                            if column_name in df.columns:  
                                # Replace the first row value with the new value
                                df[column_name].iloc[0] = round(((df[opt_platform_time].iloc[0]/df[opt_paid_time].iloc[0])-(df[baseline_platform_time].iloc[0]/df[baseline_paid_time].iloc[0]))*100,2)
                            return df

                        total_row = replace_column_value(total_row, 'Efficiency Difference','Baseline Platform Time', 'Baseline Paid Time','Optimisation Platform Time','Optimisation Paid Time')
                        average_row = replace_column_value(average_row, 'Efficiency Difference','Baseline Platform Time', 'Baseline Paid Time','Optimisation Platform Time','Optimisation Paid Time')

                         

                        # Append the "total" row to the dataframe
                        domain_dataframe = domain_dataframe.append(total_row)
                        domain_dataframe = domain_dataframe.append(average_row)
                        domain_dataframe.loc["total", 'Project Name'] = 'Total'
                        domain_dataframe.loc["average", 'Project Name'] = 'Average'

                        time_columns = []

                        # Iterate through the columns
                        for col in domain_dataframe.columns:
                            # Check if the string "Time" is in the column name
                            if "Time" in col:
                                # Append the column name to the list
                                time_columns.append(col)

                        # Print the list of columns with "Time" in the name

                        domain_dataframe[time_columns] = domain_dataframe[time_columns].applymap(lambda x: f"{int(x)//60}:{int(x)%60:02d}" if isinstance(x, (int, float)) else x)
                        
                        count_columns = []

                        for col in domain_dataframe.columns:
                            # Check if the string "Time" is in the column name
                            if "Count" in col:
                                # Append the column name to the list
                                count_columns.append(col)

                        domain_dataframe[count_columns] = domain_dataframe[count_columns].astype(int)
                        domain_dataframe = domain_dataframe.reset_index(drop=True)
                        expander=st.expander('**Preview Selected Data**')
                        expander.caption('Expected Efficiency total was calculated by:')
                        expander.code('(([sum_opt_platform_time]/[sum_opt_paid_time])-([sum_baseline_platform_time]/[sum_baseline_paid_time]))*100', language='python')

                        expander.caption('Ignore the Average value for Expected Efficiency as it is only a value as it is equal to the total')
                        expander.write(domain_dataframe)
                        domain_dataframe = domain_dataframe.fillna('')
                        
                        if file_type == 'XLSX':

                            buffer = BytesIO()
                            with ExcelWriter(buffer,engine='xlsxwriter') as writer:
                            
                                domain_dataframe.to_excel(writer,sheet_name='API Results',index=0)
                                workbook = writer.book # Access the workbook
                                num_columns = len(domain_dataframe.columns)
                                # Map the column index to the corresponding letter code
                                column_codes = [chr(65 + index) for index in range(num_columns)]
                                concatenated_codes_for_columns = '{}:{}'.format(column_codes[0], column_codes[-1])
                                worksheet= writer.sheets['API Results'] 
                                worksheet.set_column(concatenated_codes_for_columns, 20)
                                last_row = domain_dataframe.iloc[-1]
                                total_row = domain_dataframe.iloc[-2]

                                
                                last_row_index = domain_dataframe.index[-1]+1
                                total_row_index = domain_dataframe.index[-1]
                                

                                border_bold_format = workbook.add_format({'top': True, 'bottom': True, 'bold': True})

                                # Write the last row of the dataframe to the worksheet, with the border and bold format
                                worksheet.write_row(last_row_index, 0, last_row, border_bold_format)
                                worksheet.write_row(total_row_index, 0, total_row, border_bold_format)

                                border_format = workbook.add_format({'border': 1})

                    
                            
                                workbook.close()

                            ste.download_button(
                            label=f"Download XLSX Report",
                            data=buffer,
                            file_name= f"{file_title}.xlsx",
                            mime="application/vnd.ms-excel")

                        if file_type == 'CSV':
                            buffer = BytesIO()
                            domain_dataframe.to_csv(buffer, index=False)

                            ste.download_button(
                            label=f"Download CSV Report",
                            data=buffer,
                            file_name= f"{file_title}.csv",
                            mime="application/vnd.ms-excel")

                        if file_type == 'TXT':
                            buffer = BytesIO()
                            domain_dataframe.to_csv(buffer, sep='\t', index=False)

                            ste.download_button(
                            label=f"Download TXT Report",
                            data=buffer,
                            file_name= f"{file_title}.txt",
                            mime="application/vnd.ms-excel")

                        if file_type == 'KPIs':
                            #TODO: st.metric for the following differences:
                            # paid time -> value is total paid time_op and delta is paid time difference 
                            # number of duties -> value is total_duties_op and delta is duty count diff
                            # Once paid break implemented, leverage this 
                            # Any other stats that are worth rendering - after implementing in the top half of the code
                            
                            pass


                    else:
                        st.warning('Please resubmit the form, file title cannot be blank')
            
        
        

   

    
                

                

'''[api_secrets_dict]
  [[api_secrets_dict.arriva-uk-bus-midlands]]
    client_id = "5meo5bgde6qrqdv8nhdn5lavbt"
    client_secret = "gr91nepsu04tk7amicgqub46u7qbh2ken4ign3gte7oabklngjj"
  
  [[api_secrets_dict.arriva-uk-bus-northwest]]
    client_id = "lkp1ctunb52jglbrl6jnrnu0n"
    client_secret = "5h30fhbh7qso10ea6uhioglm2060v5q9idea6mo32fjgg3aoiri"
  
  [[api_secrets_dict.arriva-uk-bus-northeast]]
    client_id = "7m0anaukceo0jk54ijs4rcp309"
    client_secret = "10sdgnfjidsl3nrqvl2u3rdftjautumus947a42dt2j8lbfr86dt"
  
  [[api_secrets_dict.arriva-uk-bus-south]]
    client_id = "3o4kv9bs4kfotpr2juc2vlmhgb"
    client_secret = "pblsm1lbtmnd8321jj88tddl9nke8bnfqsjg3rg0iq26kk55qa4"
  
  [[api_secrets_dict.sg-midlands]]
    client_id = "2c6kk2a95cjk5the7m4o80dji9"
    client_secret = "16a54ce8d6ur9nuf0v4v0gkr3ttfl81m4vbi0lkirbf0q1c0mgh6"
  
  [[api_secrets_dict.sg-south]]
    client_id = "2g4dq6v6212oji5146l91rqj2g"
    client_secret = "unrokjqv9ipf5t0ep1udlvrtvtf15uq4khbpti9i5h6mgtdlku8"
  
  [[api_secrets_dict.arriva-uk-bus-london]]
    client_id = "q4d5jcjhdcide6t28ggpu2egi"
    client_secret = "1uqqgv79c0bbmoasf905a13r744qpd9t20a3438tfej4iqkh4jqd"
  
  [[api_secrets_dict.sg-eastscotland]]
    client_id = "2pa073kjtr4nl6pkp9ol5ip7i6"
    client_secret = "cgc84u2a468fhfh652mhrm9ts2s4uldpli3r1vdcql93vlfuoni"


'''


'''api_secrets_dict= {
    #DICT STRUCRURE:
    # domain name(key): (client id, client secret)
    'arriva-uk-bus-midlands':('5meo5bgde6qrqdv8nhdn5lavbt','gr91nepsu04tk7amicgqub46u7qbh2ken4ign3gte7oabklngjj'),
    'arriva-uk-bus-northwest':('lkp1ctunb52jglbrl6jnrnu0n','5h30fhbh7qso10ea6uhioglm2060v5q9idea6mo32fjgg3aoiri'),
    'arriva-uk-bus-northeast':('7m0anaukceo0jk54ijs4rcp309','10sdgnfjidsl3nrqvl2u3rdftjautumus947a42dt2j8lbfr86dt'),
    'arriva-uk-bus-south':('3o4kv9bs4kfotpr2juc2vlmhgb','pblsm1lbtmnd8321jj88tddl9nke8bnfqsjg3rg0iq26kk55qa4'), 
    'sg-midlands': ('2c6kk2a95cjk5the7m4o80dji9', '16a54ce8d6ur9nuf0v4v0gkr3ttfl81m4vbi0lkirbf0q1c0mgh6'), 
    'sg-south': ('2g4dq6v6212oji5146l91rqj2g', 'unrokjqv9ipf5t0ep1udlvrtvtf15uq4khbpti9i5h6mgtdlku8'),
    'arriva-uk-bus-london': ('q4d5jcjhdcide6t28ggpu2egi', '1uqqgv79c0bbmoasf905a13r744qpd9t20a3438tfej4iqkh4jqd'),
    'sg-eastscotland':('2pa073kjtr4nl6pkp9ol5ip7i6', 'cgc84u2a468fhfh652mhrm9ts2s4uldpli3r1vdcql93vlfuoni')
}'''

'''d = {
    "type": "service_account",
    "project_id": "scriptanalyticsoptibus",
    "private_key_id": "5d78bcc56b2e37aa9958ab4639cc0dd87f8ecf00",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCreREQzGzrfVCi\ny1u+4jKZg0WVuPZiAe57/SxkQ0gH/bTy+5WlJlpS98oA44UaQvAZyEuCs9ioIWvx\n+qs8YO3gOgQuPIRRa6kGh2NVG49nu1AqGWMM+DE6M4ms11Lzly9zJoNGM6ElW4pw\nss/ENeTAHZtYKsZFw+nwOhuLUBj79863/s7pkYH29uT2qresZBhg28aqNGy0N0bp\n2UCCQp4+TqNxcMY3bCvu8CZhKNeeLnGkONDb3VopK7THN1ieSFN+P2uXZeB2M+cb\nLqUFAinknttVDDYiZ1GK6z57HPXe1i+GWR31FRkiWOiI//ggDDKkw1b3rBdG/JXA\nWJMlaJeTAgMBAAECggEANk4n0PWlEymDZaNY4Ym6s7KG0aWb5istb7gdHH3QbLN1\nYLS7zmtolLkkEQb9avVDHC86duDlX04CZUbZf0xFWcuV8ly5HR4nEEnFdDk/v/7r\nIwIaRAvAb2nOEt64xyuc2++ZCJ0mTnIA1LNUqJPW598YiSI22/ZxeuK0QXkzCG0M\nYlDyZHRQ8Brth1M3yLQ2zsvP2DuFCgELzqxEnzXCX24MYcHSFU2Uo5pqcmA19Kph\n049GyTEQeng9m+uLuC5ZMuKRBhFPgHoS+ztiIWgQfREEFUOSqGiL6KACIYxKNPav\nudWv6a3LUl5y4IrisfX0Z3oTBayFKrpLwEbfkueN2QKBgQDfJq0/rwhtciT3XQFr\nIW3bqcF+fu3TvKCelvXBb8BH1y10MgfP9LnlglI8l0I0yoM0ylyGl61d6936Zqdo\neaeFueK0B3nB8NkbuVGyGrcT6c+1zjlzSLILn6KXQwHvsqQmLcwpdJJ0s1FroMfX\nIoNB7ptURuce/PRyKiEnHhTDiQKBgQDEtu1ivBPmzCAATtPRw6F9U8MzmfamnVB3\ncoGSjeNuLKkwSUGbGp717HPnGjfwLnlx8y/xFc6WT2nccSMpfmahrqHmZ8bmeKfi\nCtkKagqH0+0JUK5QnMZ07aWUx221aYDZVD2su0xURcMitUUlActkTR0FVtp7g7sn\n9uhxSraPOwKBgCePEbNycgoE7Cy13iSBvpnsVBL6qWqZy9eRZ2utReb2bGZm4B+r\ngLEBE1cOcXwuUmgvX4u0o9U8lRNo01CgEGSSeW/0y07oRXS2u5bZSY9MKbhPILwh\nLP8Gg6pRBKzbfGIQSFspATWS/GIaFJM82Qv9IqgkEaDasvLyKQJoQMIRAoGAJn1A\nT1xLbJIuBMFnsphbhaEZBYSvfQQ01VpDPgCjKlB0viznOEjqJ2Yfe21m2ojNXF1f\n2QUSSdi59tbwMVM5E5OOu/08LqYW+yRyORZsfw0ZSQRPRxiVwBUrm8O1lJRTELLp\n6l0vXyHTP3xGTipeuH5aGkxXLWml+fjWnCwpMjcCgYBVusFODtqdcyocSYcrcJSw\ntoBXvaoE5mLuYBNIt5uKNblaijSck4ojls/Sya3WmJpYmvVRnNvNAPdF3yPXGyPs\nnjdK4K9b9+/INQXvNPJn+ynGTV3OXFENzvmXAIfQ4Z/IPoOz1NpBXigDquJE01Lt\nUmodlIwe3yxhyb4JxPllOw==\n-----END PRIVATE KEY-----\n",
    "client_email": "scriptsanalyticsoptibus@scriptanalyticsoptibus.iam.gserviceaccount.com",
    "client_id": "117361817686052835019",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/scriptsanalyticsoptibus%40scriptanalyticsoptibus.iam.gserviceaccount.com"
  }'''


#POTENTIAL CACHE FOR CALLING THE API

'''# Hash the data to check for changes
def hash_data(data):
    return hashlib.sha256(str(data).encode()).hexdigest()

# Get the data from the sheet, or from cache if it hasn't changed
@st.cache
def get_data():
    try:
        with open('data_hash.txt', 'r') as f:
            last_hash = f.read()
    except FileNotFoundError:
        last_hash = None
    data = sheet.get_all_records()
    current_hash = hash_data(data)
    if last_hash != current_hash:
        with open('data_hash.txt', 'w') as f:
            f.write(current_hash)
        return data
    else:
        st.write('test')
        return data

data = get_data()'''