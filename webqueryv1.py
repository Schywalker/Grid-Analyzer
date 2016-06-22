# code to download json from web url

import time,requests,json,os
import numpy as np
import pandas as pd
import re
global backup_path
backup_path = r"C:\Users\rajarovan\Documents\filebackup"
class RequestError(Exception):
    pass

def _try_page(url, attempt_number=1):
    max_attempts = 3
    try:
        response = requests.get(url)
        response.raise_for_status()
    except (requests.exceptions.RequestException, socket_error, S3ResponseError, BotoServerError) as e:
        if attempt_number < max_attempts:
            attempt = attempt_number + 1
            return _try_page(url, attempt_number=attempt)
        else:
            logger.error(e)
            raise RequestError('max retries exceed when trying to get the page at %s' % url)

    return response

def _meter_reading(address,file):
    df = pd.read_csv('example.csv')
    # Make a get request to get the latest reading of the netmeter
    # The code reads the addresses stored in the json file
    response = requests.get(address+"/datalog.json?s=1")

    #print the status code of the response.
    print(response.status_code) # A success response code of 200 is expected
    
    
    data = response.json()
    metername= data["label"]
    indexofWhr = data["names"].index("WHr(A+B+C)") # get index of Whr(A+B+c):This is applicable for netmeter 3P
    meterout = pd.DataFrame(data["logdata"],columns=data["names"])
    meterout_Wh = meterout.ix[:,[0,indexofWhr]]
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = file + timestr # create a filename with the datetimestamp
    #backup of each meter reading on the day of query
    meterout_Wh.to_csv(filename + '.csv',index=False) # save the reading to the file with the datetimestamp
    common_col=data["names"][0]
    _update_meterreading(address,file,common_col,meterout_Wh)


def _update_meterreading(address,file,common_col,meterout_Wh):
    meterout_Wh['kWh'] = meterout_Wh["WHr(A+B+C)"] - meterout_Wh["WHr(A+B+C)"].shift(1) # Calculate the difference between consecutive rows.

    # if file does not exist create the file and add the dtataframe 
    if not os.path.isfile(file + '.csv'):
        # the difference between consecutive rows give the kWh consumption for that timeperiod
        meterout_Wh.to_csv(file + '.csv',index=False)

    else: # else it exists so append without writing the header
        df = pd.read_csv(file + '.csv') #read the masterfile of the given meter
        lastdate = df.iloc[-1][common_col] # last date of meter reading for the old file
        newdate = meterout_Wh.iloc[0][common_col]  # first date of meter reading for the new data

        # case when there is gap between the data recording(person has forgotten to run the prog at given interval)
        if newdate > lastdate:
            # replace the nan kWh value for the first row in the new reading
            meterout_Wh.loc[0,"kWh"] = meterout_Wh.iloc[0]["WHr(A+B+C)"]-df.iloc[-1]["WHr(A+B+C)"]
            df_new = pd.concat([df, meterout_Wh]) # append the newly read data to the masterfile

        # case when there is time overlap between the data recording
        else:
            # The index where there begins a overlap of timestamp for the old meter reading and new
            # We only need to append the new dates to the old file, so look for the last date of entry on the old file inside new file
            overlap_index = meterout_Wh.common_col[meterout_Wh.common_col==lastdate].index.tolist()[0]
            df_new  = pd.concat([df, meterout_Wh.loc[overlap_index+1:]])   # append the data after that date onwards
        df_new.to_csv(file + '.csv',index=False) #Write the updated file to csv for the meter

def _readjson(filename):
    
    # Open the json file for reading
    in_file = open(filename,"r")

    # Load the contents from the file, which creates a new dictionary
    new_dict = json.load(in_file)
    return new_dict


def main():
    # read the meter address and names
    meterinfo = _readjson("meterdata.json")
    _meter_reading(meterinfo["meteraddress"][20],meterinfo["filename"][20])
       
       #for meter , file in zip(meterinfo["meteraddress"],meterinfo["filename"]):
           #meterreading(meter,file)
       
if __name__=="__main__":      
   start_time=time.time() # notes the start time # at the beginning of run phase
   main()
   print("--- %s seconds ---" % (time.time() - start_time)) # prints the time to run the entire file
