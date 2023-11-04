########################################## Task Schedular Event ###############################################

#<-------------------------------------------- Imports -------------------------------------------------------->
import mysql.connector
from pymongo import MongoClient
import datetime
import time
from itertools import groupby
import json

# For identify the current timestamp
import datetime
import pytz
from pytz import timezone 

import logging
import traceback

filename ="schedular_demo_log/"+"all_demo_log"+str(time.time())+".log"
logging.basicConfig(filename=filename,
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#<-------------------------------------- Database connections ------------------------------------------------->

class database_connection:
  def __init__(self,sql_host = "localhost",sql_user="smtechadmin",sql_pass="admin@smtech",default_database="S1002",mongo_url ='mongodb://smtechadmin:admin%40smtech@smartories.com:27017/'):
      self.sql_host = sql_host
      self.sql_user = sql_user
      self.sql_password = sql_pass
      self.database = default_database
      self.mongo_url = mongo_url

  def connect_sql(self):
    try:
      self.sqldb = mysql.connector.connect(host = self.sql_host,
                                          user = self.sql_user,
                                          password = self.sql_password,
                                          database = "smtech_demo")
      return self.sqldb

    except:
      print("Unable to connect sql database") 

  def connect_mongo(self):
    try:
      self.mongodb = MongoClient(self.mongo_url)
      self.mdb = self.mongodb[self.database]
      # self.mdb = self.mongodb["correction_db"]
      return self.mdb
    except:
      print("Unable to connect mongodb")

#<------------------------------------------- Helper functions -------------------------------------------------->

def find_device_status(machine_gateway,f_t,t_t):
  device_gateway = machine_gateway.split("/")
  # device_gateway = "/"+device_gateway[1]+"/"+device_gateway[2]+"/"+"1DeviceStatus"
  device_gateway ="/chennai/S1002/device_status/GM001"

  db_instance = database_connection().connect_mongo()
  collection = db_instance[device_gateway]
  # start_time = "2022-12-22 09:00:00"
  # end_time = "2022-12-22 23:00:00"
  start_time = f_t
  end_time = t_t
  start_time = datetime.datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S')
  end_time = datetime.datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S')
  cur = collection.find({"updated_on":{'$gte':start_time ,'$lt':end_time}})
  lst = [i for i in cur]
  return lst

# <----------------------------------------Stored Procedure function------------------------------------------->
def stored_fun_call(production_id):
  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  cursor.callproc('child_records_create', [production_id, ])
  # for result in cursor.stored_results():
  #   pass
  #   print(result.fetchall())
  db_instance.commit()
  # if(db_instance.commit()):
  #   print("stored procedure executed")
#<------------------------------------------ insert operation --------------------------------------------------->

def info_insert_data(production_id,machine_gateway,machine_id,shift_date,calendar_date,part_id,tool_id,ppc,start_time,end_time,shot_count,no_data,shiftTimings,shift_list):
  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  machine_id = machine_id[0]
  calendar_date = calendar_date
  shift_date = shift_date
  shift_id = getShiftid(shiftTimings,shift_list,start_time)
  start_time = start_time
  end_time = end_time
  part_id = part_id
  tool_id = tool_id
  actual_shot_count = shot_count
  correction_min_counts=0
  if no_data >= 1:
    production = 0
    correction_min_counts=0
  else:
    t_f = str(calendar_date)+" "+str(start_time)
    t_e = str(calendar_date)+" "+str(end_time)
    device_state = find_device_status(machine_gateway,t_f,t_e)
    if (device_state and(device_state[-1]['data']['device_status'] == "Offline")):
      production = None
      correction_min_counts= None
    else:
      production =0
      correction_min_counts=0
  correction_min_counts = "-"+str(production)
  rejection_max_count = production

  if production != None:
    sql_query = "INSERT INTO `pdm_production_info`( `production_event_id`,`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`, `part_id`, `tool_id`, `actual_shot_count`, `production`, `correction_min_counts`,`rejection_max_counts`,`hierarchy`) VALUES(%s  ,%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)"
    val = (production_id, machine_id , calendar_date , shift_date , shift_id , start_time , end_time , part_id , tool_id , actual_shot_count , production , correction_min_counts ,rejection_max_count,"parent")
  else:
    sql_query = "INSERT INTO `pdm_production_info`( `production_event_id`,`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`, `part_id`, `tool_id`, `actual_shot_count`,`hierarchy`) VALUES(%s , %s  ,%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s)"
    val = (production_id, machine_id , calendar_date , shift_date , shift_id , start_time , end_time , part_id , tool_id , actual_shot_count,"parent")
  cursor.execute(sql_query,val)
  db_instance.commit()
  # Stored procedure function call for create the child records
  stored_fun_call(production_id)
  print("Data stored in production Info Table.....")
  return

#<------------------------------------- update operations ------------------------------------------------------->

def downtime_insert_data(machine_id,shift_date,calendar_date,part_id,tool_id,start_time):

  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  db_instance.commit()
  print("done")    

def update_previous_end_time(start_time):

  return



def split_past_future(active_records):
  present_data = []
  future_data = []
  past_data = []

  current_hour = int(int(0 if int(int(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H")))==0 else int(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H"))))-1
  current_end_hour = int(int(0 if int(int(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H")))==0 else int(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H"))))
  current_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
  if int(current_end_hour)==0:
    current_date = (datetime.datetime.strptime(current_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
  for i in active_records :
    data_arr = i['gateway_time'].split()
    gate_way_date = data_arr[0]
    gate_way_time = data_arr[1].split(':')
    gateway_date_array_split = gate_way_date.split('-')
    current_date_array_split = current_date.split('-')
    if(gate_way_date == current_date):
      if(int(gate_way_time[0]) == int(current_hour)):
        present_data.append(i)
      elif(int(gate_way_time[0])==23):
        present_data.append(i)
      elif(int(gate_way_time[0]) > int(current_hour)):
        future_data.append(i)
      else:
        past_data.append(i)

    elif((int(gateway_date_array_split[0])>int(current_date_array_split[0])) and (int(gateway_date_array_split[1])>= int(current_date_array_split[1])) and (int(gateway_date_array_split[2])>=int(current_date_array_split[2]))):
        future_data.append(i)
    else:
        past_data.append(i) 
  return present_data,past_data,future_data


def getTabledetails(machine):

  db = database_connection().connect_sql()
  mycursor= db.cursor()
  query = "SELECT t.* FROM pdm_tool_changeover as t INNER JOIN settings_machine_iot as s on t.machine_id=s.machine_id WHERE s.iot_gateway_topic = %s ORDER BY t.shift_date DESC,t.calendar_date DESC,t.event_start_time DESC,t.last_updated_on DESC LIMIT 1;"
  mycursor.execute(query,(machine,))
  shift = mycursor.fetchall()
  if len(shift)==0:
    db = database_connection().connect_sql()
    mycursor= db.cursor()
    query ="SELECT machine_id FROM `settings_machine_iot` WHERE `iot_gateway_topic`= %s ;"
    mycursor.execute(query,(machine,))
    shift = mycursor.fetchall()
    
  elif(len(shift)>0):
    mycursor = db.cursor()
    sql = "SELECT * FROM tool_changeover WHERE id=%s;"
    tool_chid = shift[0][0]
    mycursor.execute(sql,(tool_chid,))
    shift_tmp = mycursor.fetchall()
    part_arr = [] 
    for i in shift_tmp:
      part_arr.append(i[3])
    part_str = ','.join(part_arr)

    pid = part_str
    tid = shift[0][3]
    estm = shift[0][5]
    mid = shift[0][1]
    sdate = shift[0][4]
    shift_t = ()
    shift_t = [shift_t+(mid,mid,sdate,estm,pid,tid)]
    shift  = list(shift_t)
  return shift

def getTabledetails_offline(machine,gateway_time):
  time = gateway_time.split(" ")
  db = database_connection().connect_sql()
  mycursor= db.cursor()
  query = "SELECT t.machine_id,t.tool_changeover_id,t.no_of_part,t.tool_id,t.shift_date,t.calendar_date,t.event_start_time,t.shift_id,t.machine_event_id,t.last_updated_by,t.last_updated_on FROM pdm_tool_changeover as t INNER JOIN settings_machine_iot as s on t.machine_id=s.machine_id WHERE s.iot_gateway_topic = %s and t.shift_date<=%s and t.event_start_time<=%s ORDER BY t.shift_date DESC,t.event_start_time DESC, t.last_updated_on DESC LIMIT 1;"
  mycursor.execute(query,(machine,time[0],time[1],))
  
  shift = mycursor.fetchall()
  if len(shift)==0:
    db = database_connection().connect_sql()
    mycursor= db.cursor()
    query ="SELECT machine_id FROM `settings_machine_iot` WHERE `iot_gateway_topic`= %s;"
    mycursor.execute(query,(machine,))
    shift = mycursor.fetchall()
  return shift

def find_duration(start_date,end_date,start_time,end_time):
  temp_start = str(start_time).split(":")
  temp_end = str(end_time).split(":")

  x_date=str(start_date).split("-")
  y_date=str(end_date).split("-")
  a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
  b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
  c_t = b_t-a_t
  temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

  temp_min = int(temp_duration/60)
  temp_sec = int(temp_duration%60)
  duration = str(temp_min)+"."+str(temp_sec).zfill(2)
  return duration

#<--------------------------------Prodcution event id generation function--------------------------------->

# stored procedure function call
def id_generation():
  db = database_connection().connect_sql()
  mycursor= db.cursor()
  sql = "SELECT production_event_id_generation();"
  mycursor.execute(sql)
  count = mycursor.fetchall()
  # print(count[0][0])
  if len(count) > 0:
    tmp_pid = count[0][0]
    pid = "PE"+str(tmp_pid)
  else:
    tmp_pid = 0
    tmp_pid = tmp_pid+1
    pid = 1000+tmp_pid
    pid = "PE"+str(pid)
  return pid
#<------------------------------------- process data pdm_info------------------------------------------------>

def process_data_pdm_info(machine,active_records, pdm_start_time, pdm_end_time,no_data,shiftTimings,shift_list):
  present_data,past_data,future_data = split_past_future(active_records)
  shift = getTabledetails(machine)
  machine_id = shift[0]
  # calendar_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
  calendar_date = datetime.datetime.strptime(((datetime.datetime.strptime(str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S").date()

  start_time = pdm_start_time
  end_time = pdm_end_time

  # --------------------------------------- Process Previous Records
  # Process Previous One Hour Data
  current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
  prev_time = current_time - datetime.timedelta(hours=2)
  prev_time = prev_time.replace(minute=0, second=0)
  prev_time = prev_time.strftime('%Y-%m-%d %H:%M:%S')
  prev_time = datetime.datetime.strptime(str(prev_time), '%Y-%m-%d %H:%M:%S')

  active_records_preview = []
  for prev_rec in past_data:
    p_count = datetime.datetime.strptime(str(prev_rec['gateway_time']), '%Y-%m-%d %H:%M:%S')
    if p_count >= prev_time:
      active_records_preview.append(prev_rec)

  if len(active_records_preview)>0:
    date_time= datetime.datetime.strptime(active_records_preview[0]['gateway_time'], "%Y-%m-%d %H:%M:%S")
    s_time= date_time.strftime("%H")+":"+"00"+":"+"00"

    # # Find the record in info Table
    db_instance = database_connection().connect_sql()
    cursor = db_instance.cursor()
    sql_query2 = "SELECT * FROM `pdm_production_info` WHERE `machine_id`= %s and `shift_date`=%s and `start_time` =%s and `hierarchy`='parent' LIMIT 1"
    cursor.execute(sql_query2,(machine_id[0],date_time.date(),s_time,))
    previous_data = cursor.fetchone()

    if previous_data is not None:
      sql_query = "SELECT * FROM `settings_part_current` WHERE `part_id`= %s"
      cursor.execute(sql_query,(previous_data[8],))
      part_data = cursor.fetchone()
      # Shot Count
      shot_count = int(len(active_records_preview)) + int(previous_data[10])
      # Production value
      production= int(shot_count)*int(part_data[3])

      # Update the production values
      sql_query1 = "UPDATE `pdm_production_info` SET `actual_shot_count`=%s,`production`=%s, `correction_min_counts`=%s,`rejection_max_counts`=%s,`corrections`=%s,`rejections`=%s,`correction_notes`=%s,`rejections_notes`=%s,`reject_reason`=%s WHERE `r_no`=%s"
      cursor.execute(sql_query1,(shot_count,production,("-"+str(production)),production,"0","0","","","",previous_data[0],))
      db_instance.commit()

      # Update all the child records respective of the parent record.
      sql_query1 = "UPDATE `pdm_production_info` SET `actual_shot_count`=%s,`production`=%s, `correction_min_counts`=%s,`rejection_max_counts`=%s,`corrections`=%s,`rejections`=%s,`correction_notes`=%s,`rejections_notes`=%s,`reject_reason`=%s WHERE `hierarchy`=%s"
      cursor.execute(sql_query1,(shot_count,production,("-"+str(production)),production,"0","0","","","",previous_data[1],))
      db_instance.commit()

      logger.info("*********")
      logger.info("Previous Production Data updated for")
      logger.info(machine_id[0])
      logger.info("Production Count")
      logger.info(len(active_records_preview))
      logger.info("*********")

  # ------------------------------------- Previous Process Completed
  
  # end_time_t = str(calendar_date)+" "+str(end_time)
  end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(end_time)
  end_time_tmp = datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S')
  shift_date = getShiftdate(end_time_tmp)
  # Default No-tool, No-part
  part_id = "PT1001"
  tool_id = "TL1001"
  part_produced_cycle = 0

  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  sql_query2 = "SELECT * FROM `pdm_production_info` WHERE `machine_id`= %s and `shift_date`=%s and `start_time`=%s"
  cursor.execute(sql_query2,(machine_id[0],shift_date,start_time,))
  previous_data = cursor.fetchone()
  if previous_data is not None:
    logger.info("Data Duplication Occurred!")
    logger.info(datetime.datetime.now(pytz.timezone('Asia/Kolkata')))
  else:
    shot_count = len(present_data)
    ppc = int(shot_count) * int(part_produced_cycle)
    # Production id will be generated from Schedular
    production_id = id_generation()
    info_insert_data(production_id,machine,machine_id,shift_date,calendar_date,part_id,tool_id,ppc,start_time,end_time,shot_count,no_data,shiftTimings,shift_list)
  
#<------------------------------------- process data downtime ------------------------------------------------>
def process_data_pdm_downtime(machine,collection,shiftTimings,pdm_start_time,shift_list,pdm_end_time,shift_change,shiftTimings_current,shift_list_current):
  present_data_current,past_data,future_data = split_past_future(collection)

  m_gateway = machine

  shift = getTabledetails(machine)
  machine_id = shift[0][0]

  # Process the previou hour data
  current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
  prev_time = current_time - datetime.timedelta(hours=2)
  prev_time = prev_time.replace(minute=0, second=0)
  prev_time = prev_time.strftime('%Y-%m-%d %H:%M:%S')
  prev_time = datetime.datetime.strptime(str(prev_time), '%Y-%m-%d %H:%M:%S')

  present_data = []
  for prev_rec in past_data:
    p_count = datetime.datetime.strptime(str(prev_rec['gateway_time']), '%Y-%m-%d %H:%M:%S')
    if p_count >= prev_time:
      present_data.append(prev_rec)

  l= len(present_data)
  #date_temp = datetime.datetime.strptime(present_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
  machine = machine_id
  s=0
  c = 0
  t=0

  if l>0: #Condition to check whether the present data present in the present data bucket
    j=0
    flag_s=0
    timestamp = present_data[0]['gateway_time'].split(" ")
    end_time = timestamp[1]
    timestamp_gateway = present_data[0]['gateway_time']

    while(j<l): #This loop will help to cumulate the Next next occurence of the Active Data record
      t_tamp = present_data[j]['gateway_time'].split(" ")
      previous_date = timestamp[0]
      end_date = previous_date

      event = present_data[j]['status']
      event_g = event
      timestamp_gateway = present_data[j]['gateway_time']

      if(j<l):
        timestamp_gateway = present_data[j]['gateway_time']
        timestamp = present_data[j]['gateway_time'].split(" ")
        end_time = timestamp[1]
        end_date=timestamp[0]
      else:
        timestamp_gateway = present_data[j-1]['gateway_time']
        timestamp = present_data[j-1]['gateway_time'].split(" ")
        end_time = timestamp[1]
        end_date=timestamp[0]

      #condition to overcome the array index out of bound exception.....
      if j<l:
        shot_count = present_data[j]['shot_count']
      else:
        shot_count = present_data[j-1]['shot_count']
      if j==l:
        end_time = str(pdm_end_time)
      j=j+1

      event_g = event
      shiftTimings = getShiftTimings_offline(database_connection().connect_sql(),present_data[0]['gateway_time'])
      shift_list = getShiftList(shiftTimings)
      shift_id = getShiftid(shiftTimings,shift_list,end_time)

      shift = getTabledetails_offline(machine,present_data[0]['gateway_time'])
      machine_id = machine
      shift_date = getShiftdate_offline(present_data[0]['gateway_time'])
      calendar_date = datetime.datetime.strptime(present_data[0]['gateway_time'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
      source = "Offline"
      timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(end_time)), '%Y-%m-%d %H:%M:%S')
      db_instance = database_connection().connect_sql()
      cursor = db_instance.cursor()
      sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s and `timestamp`<=%s ORDER BY `timestamp` DESC LIMIT 1"
      cursor.execute(sql_query2,(machine_id,shift_date,timestamp_t,))
      previous_data = cursor.fetchone()

      if previous_data is not None:
        start_time = previous_data[10]
  
        previous_data_t=previous_data
        previous_start = previous_data[9]
        previous_end = previous_data[10]
        previous_end_t =  previous_data[10]
        previous_duration = previous_data[13]
        previous_rno = previous_data[0]
        previous_event_id = previous_data[1]

        # If previous event is same as current event, do not do anything in offline schedular.....
        if previous_data[12] != event_g:
          if previous_data[12] == "Offline":
            sql_query1 = "UPDATE `pdm_events` SET `event`=%s , `source`=%s WHERE `r_no`=%s"
            cursor.execute(sql_query1,(event_g,"Offline",previous_rno,))
            db_instance.commit()
          else:
            shift_date = previous_data[3]
            duration = find_duration(shift_date,shift_date,previous_start,end_time)
            # Update the previous record
            sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s , `source`=%s WHERE `r_no`=%s"
            cursor.execute(sql_query1,(end_time,duration,"Offline",previous_rno,))
            db_instance.commit()

            if previous_data[12] != "Active":
              sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
              cursor.execute(sql_query2,(end_time,duration,previous_event_id,))
              db_instance.commit()

            # # Update the current Record.
            start_time = end_time
            end_time = previous_end
            duration = find_duration(shift_date,shift_date,start_time,end_time)

            part_id=previous_data[7]
            tool_id=previous_data[6]
            event = event_g
            timestamp_t1 = datetime.datetime.strptime(str(str(previous_data_t[2])+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
            sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` ,`timestamp`) VALUES(%s, %s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s,%s)"
            val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event_g,duration , "0" , "0" ,part_id, tool_id , source , timestamp_t1)
            cursor.execute(sql_query,val)
            db_instance.commit()

          logger.info("*********")
          logger.info("Before One Hour Downtime record updated!")
          logger.info(machine_id)
          logger.info(event_g)
          logger.info(start_time)
          logger.info("*********")
        else:
          print("Already Exists!")
          logger.info("*********")
          logger.info("Event already Exist!")
          logger.info(machine_id)
          logger.info(event_g)
          logger.info(start_time)
          logger.info("*********")
        print("Process Completed!")
      else:
        logger.info("*********")
        logger.info("Previous data not found!")
        logger.info(machine_id)
        logger.info("*********")
        print("previous data not found!")


  # Process the Current Hour Records
  #*******************split inactive records and group active records to find duration****************************** 
  machine = m_gateway
  present_data = present_data_current
  shift = getTabledetails(machine)

  machine_id = shift[0][0]
  source = "Main"
  # calendar_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
  calendar_date = datetime.datetime.strptime(((datetime.datetime.strptime(str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S").date()

  # end_time_t = str(calendar_date)+" "+str(pdm_end_time)
  end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(pdm_end_time)
  end_time_tmp = datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S')
  shift_date = getShiftdate(end_time_tmp)

  #  Check whether the machine has the minimum of one tool changeover else it take default No Tool, No Part from Parts
  if len(shift[0])<=1:
    db_instance = database_connection().connect_sql()
    cursor = db_instance.cursor()
    cursor.execute("SELECT part_id,tool_id FROM `settings_part_current` WHERE part_name='No Part' ;")
    partDetails = cursor.fetchone()
    part_id =partDetails[0]
    tool_id = partDetails[1]
  else:
    part_id = shift[0][4]
    tool_id = shift[0][5]

  t_f = str(calendar_date)+" "+str(pdm_start_time)
  t_e = str(calendar_date)+" "+str(pdm_end_time)

  device_state = find_device_status(machine,t_f,t_e)
  serial_id = str(machine).split("/")
  for d in device_state:
    if(d['data']['device_status'] == "Offline"):
      time_update = str(d['updated_on']).split(".")
      time_update = str(time_update[0]).split(" ")
      time_update= time_update[0]+" "+time_update[1]
      time_update = datetime.datetime.strptime(str(time_update), '%Y-%m-%d %H:%M:%S')
      devicedict = { 
        'status': 'Offline',
        'shot_status': 0, 
        'shot_count': 0, 
        'machine_id': serial_id[3],
        'gateway_time': str(time_update)
      }
      present_data.append(devicedict)
    else:
      for w , z in d['data']['meta_data']['Machine_status'].items():
        if w == serial_id[3]:
          time_update = str(d['updated_on']).split(".")
          time_update = str(time_update[0]).split(" ")
          time_update= time_update[0]+" "+time_update[1]
          time_update = datetime.datetime.strptime(str(time_update), '%Y-%m-%d %H:%M:%S')
          devicedict = { 
            'status': z,
            'shot_status': 0, 
            'shot_count': 0, 
            'machine_id': serial_id[3], 
            'gateway_time': str(time_update) 
          }
          present_data.append(devicedict)
  present_data.sort(key=lambda x: datetime.datetime.strptime(str(x["gateway_time"]), "%Y-%m-%d %H:%M:%S"), reverse=False)
  start_time = pdm_start_time

  l= len(present_data)
  s=0
  c = 0
  
  if l>0: #Condition to check whether the present data present in the present data bucket
    j=0
    flag_s=0
    timestamp = present_data[0]['gateway_time'].split(" ")
    start_time = timestamp[1]
    while(j<l): #This loop will help to cumulate the Next next occurence of the Active Data record
      t_tamp = present_data[j]['gateway_time'].split(" ")
      previous_date = timestamp[0]
      end_time=start_time
      end_date = previous_date
      k=j+1  
      event = present_data[j]['status']
      if event == "Active":    
        while (k<l):
          timestamp = present_data[k]['gateway_time'].split(" ")
          end_time = timestamp[1]
          end_date=timestamp[0]
          if present_data[k]['status'] == "Active":
            k=k+1
          else:
            break
      else:
        if(k<l):
          timestamp = present_data[k]['gateway_time'].split(" ")
          end_time = timestamp[1]
          end_date=timestamp[0]
        else:
          timestamp = present_data[k-1]['gateway_time'].split(" ")
          end_time = timestamp[1]
          end_date=timestamp[0]
      #condition to overcome the array index out of bound exception.....
      if k<l:
        shot_count = present_data[k]['shot_count']
      else:
        shot_count = present_data[k-1]['shot_count']
      j=k
      if j==l:
        end_time = str(pdm_end_time)
      temp_start = str(start_time).split(":")
      temp_end = str(end_time).split(":")

      t_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
      t_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))
      # temp_duration = int(int(int(t_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(t_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
      # if temp_duration<0:
      x_date= str(previous_date).split("-")
      y_date= str(end_date).split("-")
      a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
      b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
      c_t = b_t-a_t
      temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
      
      temp_min = int(temp_duration/60)
      temp_sec = int(temp_duration%60)
      duration = str(temp_min)+"."+str(temp_sec).zfill(2)

      reason_mapped = 0; # This is the default value for the parameter
      is_split = 0; # This is the default value for the parameter
      shift_id = getShiftid(shiftTimings,shift_list,start_time)
      # Logics for find the last record 
      if c == 0: #This condition for update the each first record of the each shift hour
        # Find the shift starting hours
        for d in shiftTimings:
          d = str(d).split(":")
          x = str(pdm_end_time).split(":")
          d = int(int(24 if int(d[0])==0 else d[0]))*3600+int(d[1])*60+int(d[2])
          x = int(int(24 if int(x[0])==0 else x[0])-1)*3600+int(x[1])*60+int(x[2])
          if int(d) == int(x):
            shift_list_list = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
            list_index = shift_list_list.index(shift_id)
            shift_start_duration = shiftTimings[list_index]
            if(list_index == 0):
              shift_end_duration = shiftTimings[len(shiftTimings)-1]
            else:
              shift_end_duration = shiftTimings[list_index-1]

            db_instance = database_connection().connect_sql()
            cursor = db_instance.cursor()

            # This will removed after testing
            # sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s ORDER BY r_no DESC LIMIT 1"
            # cursor.execute(sql_query2,(machine_id,shift_date,))
            
            timestamp_new = datetime.datetime.strptime(str(str(calendar_date)+" "+str(end_time)), '%Y-%m-%d %H:%M:%S')
            sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s and `timestamp`<=%s ORDER BY `timestamp` DESC LIMIT 1"
            cursor.execute(sql_query2,(machine_id,shift_date,timestamp_new,))

            previous_data = cursor.fetchone()
            

            if previous_data is not None:
              previous_start = previous_data[9]
              previous_end = previous_data[10]
              previous_duration = previous_data[13]
              previous_rno = previous_data[0]
              previous_event_id = previous_data[1]

              temp_start = str(previous_start).split(":")
              if previous_data[12]==event:
                temp_end = str(end_time).split(":")
                flag_s=1
              else:
                temp_end = str(start_time).split(":")

              temp_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
              temp_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

              # temp_duration = int(int(int(temp_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(temp_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
              # if temp_duration<0:
              x_date=str(previous_data[2]).split("-")
              y_date=str(end_date).split("-")
              a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
              b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
              c_t = b_t-a_t
              temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
              
              temp_min = int(temp_duration/60)
              temp_sec = int(temp_duration%60)
              pre_duration = str(temp_min)+"."+str(temp_sec).zfill(2)

              if previous_data[12]==event:
                # Update the previous shift end Record......
                sql_query1 = "UPDATE `pdm_events` SET `shot_count`=%s,`end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(shot_count,end_time,pre_duration,previous_rno,))
                db_instance.commit()

                # Update it in Reason mapping Table
                if previous_data[12] != "Active":
                  sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query2,(end_time,pre_duration,previous_event_id,))
                  db_instance.commit()
              else:
                # Update the previous shift end Record......
                sql_query1 = "UPDATE `pdm_events` SET `shot_count`=%s,`end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(shot_count,start_time,pre_duration,previous_rno,))
                db_instance.commit()

                # Update it in Reason mapping Table
                if previous_data[12] != "Active":
                  sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query2,(start_time,pre_duration,previous_event_id,))
                  db_instance.commit()
              c = 1
      db_instance = database_connection().connect_sql()
      cursor = db_instance.cursor()
      temp_var=0
      temp_var_same=0
      if(s==0 and c==0): # This loop will execute only once to update the previous inserted data record at the begining of the each hour execution
        cursor.execute("SELECT * FROM `pdm_events` WHERE `machine_id` like %s ORDER BY r_no DESC LIMIT 1;",(('%'+machine_id,)))
        previous_data = cursor.fetchone()
        if previous_data is not None: #Condition if the first data not inserted (If already data exist condition true otherwise false)
          previous_start = previous_data[9]
          previous_end = previous_data[10]
          previous_duration = previous_data[13]
          previous_rno = previous_data[0]
          previous_event_id = previous_data[1]
          temp_start = str(previous_start).split(":")
          temp_end = str(start_time).split(":")
          temp_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
          temp_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

          # temp_duration = int(int(int(temp_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(temp_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
          # if temp_duration<0:
          x_date= str(previous_data[2]).split("-")
          y_date= str(end_date).split("-")
          a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
          b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
          c_t = b_t-a_t
          temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
          
          temp_min = int(temp_duration/60)
          temp_sec = int(temp_duration%60)
          pre_duration = str(temp_min)+"."+str(temp_sec).zfill(2)

          if previous_data[12]==event: # Conditions for check the previous data event same as current event.......
            temp_end = str(end_time).split(":")
            temp_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
            temp_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

            # temp_duration = int(int(int(temp_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(temp_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
            # if temp_duration<0:
            x_date= str(previous_data[2]).split("-")
            y_date= str(end_date).split("-")
            a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
            b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
            c_t = b_t-a_t
            temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

            temp_min = int(temp_duration/60)
            temp_sec = int(temp_duration%60)
            pre_duration = str(temp_min)+"."+str(temp_sec).zfill(2)
            # print(start_time," ",end_time," ",previous_data[9])
            sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
            cursor.execute(sql_query1,(end_time,pre_duration,previous_rno,))
            db_instance.commit()
            if previous_data[12] != "Active":
              sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
              cursor.execute(sql_query2,(end_time,pre_duration,previous_event_id,))
              db_instance.commit()
            temp_var=1
          else: # If previous and Current data event not same, insert the current data
            sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
            cursor.execute(sql_query1,(start_time,pre_duration,previous_rno,))
            db_instance.commit()
            if previous_data[12] != "Active":
              sql_query1 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
              cursor.execute(sql_query1,(start_time,pre_duration,previous_event_id,))
              db_instance.commit()

          temp_var_same=1
        s=j
      # This is for insert the current data records
      # update the no data issue
      if (j==l and flag_s!=1): # Conditions to check if the current record is the last record of the hour......
        tmp_var2=0
        for idx, s_time in enumerate(shiftTimings):
          if((datetime.datetime.strptime(str(s_time), "%H:%M:%S").time().hour == (datetime.datetime.strptime(str(pdm_end_time), "%H:%M:%S").time().hour)) or shift_change==1):
            
            if shift_change == 1:
              s_time = shiftTimings_current[0]

            temp_start = str(start_time).split(":")
            temp_end = str(s_time).split(":")

            t_end_h =  int(int(24 if int(temp_end[0])==0 else int(temp_end[0])))
            t_start_h = int(int(24 if int(temp_start[0])==0 else int(temp_start[0])))

            # temp_duration = int(int(int(t_end_h)*3600)+int(int(temp_end[1])*60+int(temp_end[2])))-int(int(int(t_start_h)*3600)+int(int(temp_start[1])*60+int(temp_start[2])))
            # if temp_duration<0:
            x_date= str(previous_date).split("-")
            y_date= str(end_date).split("-")
            a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
            b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
            c_t = b_t-a_t
            temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())

            temp_min = int(temp_duration/60)
            temp_sec = int(temp_duration%60)
            duration = str(temp_min)+"."+str(temp_sec).zfill(2)
            end_time =s_time
            if temp_var_same !=0:
              cursor.execute("SELECT * FROM `pdm_events` WHERE `machine_id` like %s ORDER BY r_no DESC LIMIT 1;",(('%'+machine_id,)))
              previous_data = cursor.fetchone()
              
              previous_rno = previous_data[0]
              previous_event_id = previous_data[1]
              if previous_data[12]==event:
                duration = find_duration(previous_data[2],end_date,previous_data[9],s_time)
                sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(s_time,duration,previous_rno,))
                db_instance.commit()
                if previous_data[12] != "Active":
                  sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query2,(s_time,duration,previous_event_id,))
                  db_instance.commit()
              else:
                duration = find_duration(previous_data[2],end_date,previous_data[9],start_time)
                sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
                cursor.execute(sql_query1,(start_time,duration,previous_rno,))
                db_instance.commit()
                if previous_data[12] != "Active":
                  sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
                  cursor.execute(sql_query2,(start_time,duration,previous_event_id,))
                  db_instance.commit()

                duration = find_duration(end_date,end_date,start_time,s_time)
                # Newly added condition
                timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
                sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` ,`timestamp`) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s, %s)"
                val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id, source,timestamp_t)
                cursor.execute(sql_query,val)
              
            else:
             # Insert the current shift record of last record
              timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
              sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source`, `timestamp` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s, %s)"
              val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id, source, timestamp_t)
              cursor.execute(sql_query,val)
          
            # Next Shift First Record
            start_time=s_time
            end_time=s_time
            duration=0
            if shift_change==1:
              shift_id = getShiftid(shiftTimings_current,shift_list_current,start_time)
            else:
              shift_id = getShiftid(shiftTimings,shift_list,start_time)

            # For break the loop
            shift_change=0

            # end_time_t = str(calendar_date)+" "+str(end_time)
            end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(end_time)
            shift_date = getShiftdate(datetime.datetime.strptime(((datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S"))
            timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
            sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` , `timestamp`) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s ,%s)"
            val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id, source, timestamp_t)
            cursor.execute(sql_query,val)
            db_instance.commit()
            tmp_var2=1
            break
        if ((temp_var !=1) and (tmp_var2 !=1)):
          timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
          sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` , `timestamp` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s, %s)"
          val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id, source , timestamp_t )
          cursor.execute(sql_query,val)
          db_instance.commit()
      elif(temp_var !=1 and flag_s!=1):
        timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
        sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` , `timestamp` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s, %s)"
        val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id, source, timestamp_t )
        cursor.execute(sql_query,val)
        db_instance.commit()
      
      flag_s=0
      start_time = str(end_time)
      print("Data stored in downtime event tables......")
  
  else:
    temp_var_find=0
    for idx, s_time in enumerate(shiftTimings):  
      if((datetime.datetime.strptime(str(s_time), "%H:%M:%S").time().hour == (datetime.datetime.strptime(str(pdm_end_time), "%H:%M:%S").time().hour)) or shift_change==1):
            
        if shift_change == 1:
          s_time = shiftTimings_current[0]


        db_instance = database_connection().connect_sql()
        cursor = db_instance.cursor()

        # This will removed after testing
        # sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s ORDER BY r_no DESC LIMIT 1"
        # cursor.execute(sql_query2,(machine_id,shift_date,))
            
        timestamp_new = datetime.datetime.strptime(str(str(calendar_date)+" "+str(end_time)), '%Y-%m-%d %H:%M:%S')
        sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s and `timestamp`<=%s ORDER BY `timestamp` DESC LIMIT 1"
        cursor.execute(sql_query2,(machine_id,shift_date,timestamp_new,))

        previous_data = cursor.fetchone()
        temp_var_find=1
        if previous_data is not None:
          previous_start = previous_data[9]
          previous_end = previous_data[10]
          previous_duration = previous_data[13]
          previous_rno = previous_data[0]
          previous_event_id = previous_data[1]

          # x_d = str(device_state['updated_on']).split(".")
          # x_d = str(x_d[0]).split(" ")
          # x_d = x_d[0]+" "+x_d[1]

          # last_status_time = datetime.datetime.strptime(str(x_d), '%Y-%m-%d %H:%M:%S')
          # last_status_time = str(last_status_time).split(" ")
          
          # if device_state['data']['device_status']=="Online":
          duration = find_duration(shift_date,shift_date,previous_start,s_time)
          end_time =s_time

          sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
          cursor.execute(sql_query1,(end_time,duration,previous_rno,))
          db_instance.commit()

          # Update it in Reason mapping Table
          if previous_data[12] != "Active":
            sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
            cursor.execute(sql_query2,(end_time,duration,previous_event_id,))
            db_instance.commit()
          event = previous_data[12]

          start_time=s_time
          end_time=s_time
          duration=0
          shot_count=0

          # Insert the records as next shift first record
          if shift_change==1:
            shift_id = getShiftid(shiftTimings_current,shift_list_current,start_time)
          else:
            shift_id = getShiftid(shiftTimings,shift_list,start_time)

          # For break the loop
          shift_change=0

          # end_time_t = str(calendar_date)+" "+str(end_time)
          end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(end_time)
          shift_date = getShiftdate(datetime.datetime.strptime(((datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S"))
          
          timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
          sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` ,`timestamp`) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s ,%s)"
          val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id,source ,timestamp_t)
          cursor.execute(sql_query,val)
          db_instance.commit()
        else:
          shift_id = getShiftid(shiftTimings,shift_list,pdm_start_time)
          shift_list_list = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
          list_index = shift_list_list.index(shift_id)
          shift_start_duration = shiftTimings[list_index]
          # Function for find the duration of the event
          # if device_state['data']['device_status']=="Online":
          duration = find_duration(shift_date,shift_date,shift_start_duration,s_time) 
          shot_count = 0;
          start_time =shift_start_duration;
          end_time = s_time
          event=  "Inactive"; #This will consider as default.
          timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
          sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`,`source`, `timestamp` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s,%s , %s)"
          val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id,source , timestamp_t)
          cursor.execute(sql_query,val)
          db_instance.commit()
          
          # Insert the records as next shift first record
          start_time=s_time
          end_time=s_time
          duration=0

          if shift_change==1:
            shift_id = getShiftid(shiftTimings_current,shift_list_current,start_time)
          else:
            shift_id = getShiftid(shiftTimings,shift_list,start_time)

          # For break the loop
          shift_change=0

          # end_time_t = str(calendar_date)+" "+str(end_time)
          end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(end_time)
          shift_date = getShiftdate(datetime.datetime.strptime(((datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S"))
          # shift_date = getShiftdate()
          timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
          sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id` , `source`, `timestamp`) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s,%s,%s)"
          val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id,source, timestamp_t)
          cursor.execute(sql_query,val)
          db_instance.commit()
    if(temp_var_find !=1):
      db_instance = database_connection().connect_sql()
      cursor = db_instance.cursor()

      # This will removed after testing
      # sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s ORDER BY r_no DESC LIMIT 1"
      # cursor.execute(sql_query2,(machine_id,shift_date,))
            
      timestamp_new = datetime.datetime.strptime(str(str(calendar_date)+" "+str(end_time)), '%Y-%m-%d %H:%M:%S')
      sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s and `timestamp`<=%s ORDER BY `timestamp` DESC LIMIT 1"
      cursor.execute(sql_query2,(machine_id,shift_date,timestamp_new,))

      previous_data = cursor.fetchone()

      if previous_data is not None:
        previous_start = previous_data[9]
        previous_end = previous_data[10]
        previous_duration = previous_data[13]
        previous_rno = previous_data[0]
        previous_event_id = previous_data[1]

        # if device_state['data']['device_status']=="Online":
        duration = find_duration(shift_date,shift_date,previous_start,pdm_end_time)
        end_time =pdm_end_time

        sql_query1 = "UPDATE `pdm_events` SET `end_time`=%s,`duration`=%s WHERE `r_no`=%s"
        cursor.execute(sql_query1,(end_time,duration,previous_rno,))
        db_instance.commit()

        # # Update it in Reason mapping Table
        if previous_data[12] != "Active":
          sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s"
          cursor.execute(sql_query2,(end_time,duration,previous_event_id,))
          db_instance.commit()
        
      else:
        start_time = pdm_start_time
        end_time = pdm_end_time
        shot_count = 0
        # event = "Offline"
        event = "Inactive"
        duration = 0
        shift_id = getShiftid(shiftTimings,shift_list,start_time)
        timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')
        sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`,`source`, `timestamp` ) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s , %s)"
        val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id,source , timestamp_t)
        cursor.execute(sql_query,val)
        db_instance.commit()
    print("Data stored in downtime event tables......")
  
  return


#<------------------------------------- process data ------------------------------------------------>

def process_data(shift_list_current,shift_change,shiftTimings_current,shiftTimings,machine,collection,shift_list, duration_start = 0, duration_end = 0):

  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(duration_end != 0):
    e_hrs = s_hrs

  pdm_start_time = str(s_hrs).zfill(2)+":"+str(duration_start).zfill(2)+":"+"00"
  pdm_end_time = str(e_hrs).zfill(2)+":"+str(duration_end).zfill(2)+":"+"00"
  active_records = []
  collection.sort(key=lambda x: x["status"])
  groups = groupby(collection, lambda x: x['status'])
  for status, group in groups:
    if(status == "Active"):
      for content in group:
        active_records.append(content)  
  process_data_pdm_info(machine,active_records,pdm_start_time,pdm_end_time,len(collection),shiftTimings,shift_list)

  collection.sort(key=lambda x: x["gateway_time"])

  process_data_pdm_downtime(machine,collection,shiftTimings,pdm_start_time,shift_list,pdm_end_time,shift_change,shiftTimings_current,shift_list_current)
  print("Process Completed!")

  return 1

#<------------------------------------- on 14/07/2022 ------------------------------------------------>

#<------------------------------------- Get shift info ------------------------------------------------>

def getShiftinfo(db_instance):
  cursor = db_instance.cursor()
  # cursor.execute("SELECT LAST_VALUE(shift_log_id) AS shift FROM `settings_shift_management` ;")
  cursor.execute("SELECT shift_log_id AS shift FROM `settings_shift_management` ORDER BY last_updated_on DESC LIMIT 1")
  shift_log_id = cursor.fetchall()
  shift_log_id = shift_log_id[len(shift_log_id)-1]
  shift_log_id = shift_log_id[0].split("f")
  shift_suffix = shift_log_id[1]
  return shift_suffix

def getShiftinfo_offline(db_instance,gateway_time):
  cursor = db_instance.cursor()
  cursor.execute("SELECT `shift_log_id`  FROM `settings_shift_management` WHERE `last_updated_on`<= %s ORDER BY `last_updated_on` DESC LIMIT 1;",(gateway_time,))
  shift_log_id = cursor.fetchall()
  if len(shift_log_id)>0:
    shift_log_id = shift_log_id[0][0].split("f")
    shift_suffix = shift_log_id[1]
    return shift_suffix
  else:
    shift_suffix="01"
    return shift_suffix

#<------------------------------------- on 11/07/2022 ---------------------------------------------------->


#<------------------------------------- Get shift Timings ------------------------------------------------>
  
def getShiftTimings(db_instance):
  cursor = db_instance.cursor()
  sql_query = "SELECT * FROM `settings_shift_table` WHERE `Shifts` like %s"
  cursor.execute(sql_query,(('%'+getShiftinfo(database_connection().connect_sql()),)))
  shifts = cursor.fetchall()
  cursor.close()
  arr = []
  for count, value in enumerate(shifts):
    arr.append((datetime.datetime.min + value[1]).time())
  return arr

def getShiftTimings_offline(db_instance,gateway_time):
  cursor = db_instance.cursor()
  sql_query = "SELECT * FROM `settings_shift_table` WHERE `Shifts` like %s"
  cursor.execute(sql_query,(('%'+getShiftinfo_offline(database_connection().connect_sql(),gateway_time),)))
  shifts = cursor.fetchall()
  cursor.close()
  arr = []

  for count, value in enumerate(shifts):
    arr.append((datetime.datetime.min + value[1]).time())
  return arr

#<------------------------------------- on 12/07/2022 --------------------------------------------------->

#<------------------------------------- Get shift id ------------------------------------------------>
def getShiftid(shiftTimings,shift_list,time):
  shift="A"
  for s in shiftTimings:
    time=datetime.datetime.strptime(str(time), "%H:%M:%S").time()
    if int(time.hour) == int(s.hour):
      if (int(s.minute)>0) and (int(time.minute)>=int(s.minute)):
        shift = shift_list[str(time.hour).zfill(2)+":"+str(s.minute).zfill(2)+":00"]
        break
      elif (int(s.minute)>0) and (int(time.minute)>=int(s.minute)):
        shift = shift_list[str(str(time.hour).zfill(2)+":00:00")]
        break
    else:
      shift = shift_list[str(str(time.hour).zfill(2)+":00:00")]
  return shift

#<------------------------------------- on 12/07/2022 --------------------------------------------------->



#<------------------------------------- Get Machine info ------------------------------------------------>
def getMachineinfo(db_instance):
  cursor = db_instance.cursor()
  query = "SELECT `iot_gateway_topic` FROM `settings_machine_iot`"
  cursor.execute(query)
  machines = cursor.fetchall()
  arr = []
  for count, value in enumerate(machines):
    arr.append(value)
  return arr  


#<------------------------------------- on 12/07/2022 --------------------------------------------------->


#<------------------------------------- Get shift date -------------------------------------------------->

def getShiftdate(hour):
  shiftTimings = getShiftTimings(database_connection().connect_sql())
  hour_now = hour
  shift_date = hour_now.strftime("%Y-%m-%d")
  if(int(hour_now.strftime("%H")) <= int(shiftTimings[0].strftime("%H")) and int(hour_now.strftime("%M")) <= int(shiftTimings[0].strftime("%M"))):
    shift_date =  datetime.datetime.strptime(((datetime.datetime.strptime(str(hour), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S")
    shift_date = shift_date.strftime("%Y-%m-%d")
  return shift_date

def getShiftdate_offline(gateway_time):
  shiftTimings = getShiftTimings_offline(database_connection().connect_sql(),gateway_time)
  hour_now = datetime.datetime.strptime(gateway_time, "%Y-%m-%d %H:%M:%S")
  shift_date = hour_now.strftime("%Y-%m-%d")
  now = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
  s_time = datetime.datetime.strptime(str(hour_now.date())+" "+str(shiftTimings[0]), "%Y-%m-%d %H:%M:%S")
  # if(int(hour_now.strftime("%H")) <= int(shiftTimings[0].strftime("%H")) and int(hour_now.strftime("%M")) <= int(shiftTimings[0].strftime("%M"))):
  if(hour_now <= s_time):
    shift_date =  datetime.datetime.strptime(((datetime.datetime.strptime(str(hour_now), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S")
    shift_date = shift_date.strftime("%Y-%m-%d")
  return shift_date

#<------------------------------------- on 13/07/2022 -------------------------------------------------->

# <----------------------------------------------created by naveen status ADDING ROW DATA-------------------------------------------------------->
def add_status_raw_data(lst):
    working_list = []
    
    for i in lst:
        # print(i)
        if i['downtime_status'] == True and i['machine_status'] == True:
            status = "Inactive"
        elif i['downtime_status'] == False and i['machine_status'] == True:
            status = "Active"
        else:
            status = "Machine OFF"

        data_list = {
            "downtime_status" : i['downtime_status'],
            "machine_status" : i['machine_status'],
            "machine_id" : i['machine_id'],
            "shot_count" : i['shot_count'],
            "shot_status" : i['shot_staus'],
            "gateway_time" : i['gateway_time'],
            "status" : status,
        }
        # print(data_list)
        working_list.append(data_list)
    return working_list

# <---------------------------------------------------END OF STATUS ADDING FUNCTION---------------------------------------------------->

#<------------------------------------- Get rawData ----------------------------------------------------->

def getRawData(machine,split = 0,split_start = 0, split_end = 0):

  now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
  db_instance = database_connection().connect_mongo()
  collection = db_instance[machine]
  s_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))-1).zfill(2)
  e_hrs = now.strftime("%H")

  if(split!= 0 and int(split_end) != 0):
    e_hrs = s_hrs

  # start_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  end_time = (now.strftime("%Y"))+"-"+(now.strftime("%m"))+"-"+(now.strftime("%d"))+" "+str(e_hrs).zfill(2)+":"+str(split_end).zfill(2)+":"+"00"
  temp_start = (datetime.datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
  start_time = str(datetime.datetime.strptime(str(temp_start), '%Y-%m-%d %H:%M:%S').date())+" "+str(s_hrs).zfill(2)+":"+str(split_start).zfill(2)+":"+"00"
  
  start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
  end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
  cur = collection.find({"updated_on":{'$gte':start_time ,'$lt':end_time}})
  lst = [i['data'] for i in cur]
  return lst

#<------------------------------------- on 13/07/2022 ------------------------------------------------>
def getNoOfHoursPerShift(start,end):
  start_delta = datetime.timedelta(hours=start.hour, minutes=start.minute, seconds=start.second)
  end_delta = datetime.timedelta(hours=end.hour, minutes=end.minute, seconds=end.second)
  return end_delta - start_delta  


def update_list(startTime,noOfHours,id_v,shift_dict,end):
  startTime = (datetime.datetime.min + startTime).time()
  if int(startTime.minute)>0:
    shift_dict[str(str(startTime.hour).zfill(2)+":"+str(startTime.minute).zfill(2)+":00")]=id_v
  else:
    shift_dict[str(str(startTime.hour).zfill(2)+":00:00")]=id_v
  for i in range(1,int(noOfHours.hour)+1):
    if int(startTime.hour+i)>23:
      t = (int(startTime.hour+i)%24)
      if shift_dict[str(str(t).zfill(2)+":00:00")]=="":
        shift_dict[str(str(t).zfill(2)+":00:00")]=id_v 
    else:
      if shift_dict[str(str(startTime.hour+i).zfill(2)+":00:00")]=="":
        shift_dict[str(str(startTime.hour+i).zfill(2)+":00:00")]=id_v
  return shift_dict

# Function for find the shift Shift respective of the timings
def getShiftList(shiftTimings):

  id_v = 'A'
  shift_dict = {}
  for i in range(0,24):
    shift_dict[str(str(i).zfill(2)+":00:00")]=""
  for i in range (0,len(shiftTimings)):
    try:
      try:
        no = str(getNoOfHoursPerShift(shiftTimings[i],shiftTimings[i+1])).split(", ")[1].split(":")
        noOfHours = datetime.timedelta(hours=int(no[0]),minutes=int(no[1]),seconds=int(no[2]))
      except:
        noOfHours = getNoOfHoursPerShift(shiftTimings[i],shiftTimings[i+1])
    except:
      noOfHours = getNoOfHoursPerShift(shiftTimings[i],shiftTimings[0])
    ct = datetime.timedelta(hours=shiftTimings[i].hour, minutes=shiftTimings[i].minute, seconds=shiftTimings[i].second)
    try:
      noOfHours = (datetime.datetime.min + noOfHours).time()
    except:
      noOfHours = str(noOfHours).split(", ")[1]
      noOfHours=datetime.datetime.strptime(noOfHours, "%H:%M:%S").time()
    if(noOfHours.minute==0):
      shift_dict = update_list(ct,noOfHours,id_v,shift_dict,0)
    else:
      shift_dict = update_list(ct,noOfHours,id_v,shift_dict,2)

    id_v = chr(ord(id_v) + 1)
  return shift_dict

#<------------------------------------- Main Function ------------------------------------------------>

if __name__ == '__main__':

  shiftTimings = getShiftTimings(database_connection().connect_sql())
  shift_hours = [int(i.strftime("%H")) for i in shiftTimings]
  shift_min = [int(i.strftime("%M")) for i in shiftTimings]
  shift_list = getShiftList(shiftTimings)

  #<---------------------- Loop break daywise ------------------------->
  while(True):
    now = datetime.datetime.now(pytz.timezone('Asia/Kolkata')) # Take current time to check the current shift hours
    shift_change=0
    try:
      if(int(now.strftime("%M")) ==0): # you can change time here to run the code

        logger.info("connected")
        logger.info(now)

        # Get Machine Records from Schedular for every hour
        machines = getMachineinfo(database_connection().connect_sql())

        logger.info("Execution Start")
        logger.info(now)

        # This is for break the program if shift timing changed!
        shiftTimings_current = getShiftTimings(database_connection().connect_sql())
        shift_list_current = getShiftList(shiftTimings_current)
        if ((int(shiftTimings[0].strftime("%H")) != int(shiftTimings_current[0].strftime("%H"))) and (int(shiftTimings_current[0].strftime("%H")) == int(now.strftime("%H")))):
          shift_change=1
    #<------------------- Machine wise data processing ------------------->
        for count,value in enumerate(machines):

          machine = value[0]
          if(int(shiftTimings[0].strftime("%M")) != 0 and (int(now.strftime("%H"))-1) in shift_hours):

            for i in range(2):
              if(i==0):
                print("1")
                collection = getRawData(machine,split = 1, split_start = 0, split_end = shiftTimings[0].strftime("%M"))
                process_data(shift_list_current,shift_change,shiftTimings_current,shiftTimings,machine,collection,shift_list, duration_start = 0, duration_end = shiftTimings[0].strftime("%M"))
              else:
                print("2")
                collection = getRawData(machine,split = 1, split_start = shiftTimings[0].strftime("%M"), split_end = 0)
                process_data(shift_list_current,shift_change,shiftTimings_current,shiftTimings,machine,collection,shift_list, duration_start = shiftTimings[0].strftime("%M"), duration_end = 0)
          else:
            print("trigger")
            collection = getRawData(machine)
            process_data(shift_list_current,shift_change,shiftTimings_current,shiftTimings,machine,collection,shift_list, duration_start = 0, duration_end = 0)

          # print(f'{0} completed',(machine))

  #<------------------------------------- end of processing hourly ------------------------------------------------>
        logger.info("program execution completed")
        logger.info(now)

        # This is the line which help to break the loop to retrive machine,shift data again
        if((int(shiftTimings[0].strftime("%H")) == (int(now.strftime("%H")))) and int(shiftTimings[0].strftime("%M")) == 0):
          logger.info("Program Terminated!")
          time.sleep(65)
          break
        elif((int(shiftTimings[0].strftime("%H")) == (int(now.strftime("%H")))) and int(shiftTimings[0].strftime("%M")) != 0):
          while(True):
            now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
            if(int(now.strftime("%S")) == 0):
              if((int(shiftTimings[0].strftime("%H")) == (int(now.strftime("%H")))) and (int(shiftTimings[0].strftime("%M"))== (int(now.strftime("%M"))))):
                logger.info("Program Terminated!")
                break
          time.sleep(65)
          break

        # This is for break the program if shift timing changed!
        if shift_change==1:
          logger.info("Program Terminated due to Shift Timing changes!")
          time.sleep(65)
          break

        time.sleep(65)

    except BaseException as err:
      print(err)
      print(traceback.format_exc())
      logger.info(err)
      logger.info(traceback.format_exc()) 
      time.sleep(60)             
      


#<------------------------------------------------------ end ---------------------------------------------------->



