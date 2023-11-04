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

filename ="S1002/event_schedular_log/"+"all_prod_log"+str(time.time())+".log"
logging.basicConfig(filename=filename,
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#<-------------------------------------- Database connections ------------------------------------------------->

class database_connection:
  def __init__(self,sql_host = "localhost",sql_user="root",sql_pass="quantanics123",default_database="S1002",mongo_url ='mongodb://admin:Plmqaz%40123@165.22.208.52:27017/'):
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
                                          # database = self.database)
                                          database = "S1002")
      return self.sqldb

    except:
      print("Unable to connect sql database") 
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

def info_insert_data(production_id,machine_gateway,machine_id,shift_date,calendar_date,start_time,part_id,tool_id,end_time,shiftTimings,shift_list):
  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  machine_id = machine_id[0]
  calendar_date = calendar_date
  shift_date = shift_date
  shift_id = getShiftid(shiftTimings,shift_list,start_time)
  start_time = start_time
  end_time = end_time
  actual_shot_count = 0;

  sql_query = "INSERT INTO `pdm_production_info`( `production_event_id`,`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`, `part_id`, `tool_id`, `actual_shot_count`,`hierarchy`) VALUES(%s , %s  ,%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s)"
  val = (production_id, machine_id , calendar_date , shift_date , shift_id , start_time , end_time , part_id , tool_id , actual_shot_count,"parent")
  cursor.execute(sql_query,val)
  db_instance.commit()
  # Stored procedure function call for create the child records
  stored_fun_call(production_id)

  print("Data stored in production Info Table.....")
  return
 
def getTabledetails(machine):

  db = database_connection().connect_sql()
  mycursor= db.cursor()
  query = "SELECT t.* FROM pdm_tool_changeover as t INNER JOIN settings_machine_iot as s on t.machine_id=s.machine_id WHERE s.iot_gateway_topic = %s ORDER BY t.shift_date DESC,t.event_start_time DESC,t.last_updated_on DESC LIMIT 1;"
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

def process_data_pdm_info(machine, pdm_start_time, pdm_end_time,shiftTimings,shift_list):
  shift = getTabledetails(machine)
  machine_id = shift[0]
  # calendar_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
  calendar_date = datetime.datetime.strptime(str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")), '%Y-%m-%d %H:%M:%S').date()

  start_time = pdm_start_time
  end_time = pdm_end_time
  
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
    part_id = previous_data[8]
    tool_id = previous_data[9]
    print("Record already Exist!")
    logger.info("Data Already Exist!")
    logger.info(datetime.datetime.now(pytz.timezone('Asia/Kolkata')))
  else:
    # Production id will be generated from Schedular
    production_id = id_generation()
    info_insert_data(production_id,machine,machine_id,shift_date,calendar_date,start_time,part_id,tool_id,end_time,shiftTimings,shift_list)
  
def get_child_duration(m_event_id,e_date,e_time):
  db_instance = database_connection().connect_sql()
  cursor = db_instance.cursor()
  sql_query2 = "SELECT * FROM `pdm_downtime_reason_mapping` WHERE `machine_event_id`= %s ORDER BY start_time DESC LIMIT 1"
  cursor.execute(sql_query2,(m_event_id,))
  previous_data = cursor.fetchone()

  temp_start = str(previous_data[7]).split(":")
  temp_end = str(e_time).split(":")

  x_date=str(previous_data[4]).split("-")
  y_date=str(e_date).split("-")
  a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
  b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
  c_t = b_t-a_t
  temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
  return temp_duration
#<------------------------------------- process data downtime ------------------------------------------------>
def process_data_pdm_downtime(machine,shiftTimings,pdm_start_time,shift_list,pdm_end_time):
  shift = getTabledetails(machine)
  machine_id = shift[0][0]
  source = "Schedular"
  # calendar_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
  calendar_date = datetime.datetime.strptime(((datetime.datetime.strptime(str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")),"%Y-%m-%d %H:%M:%S").date()

  end_date = datetime.datetime.strptime(str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")), '%Y-%m-%d %H:%M:%S').date()

  # end_time_t = str(calendar_date)+" "+str(pdm_end_time)
  end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(pdm_end_time)
  end_time_tmp = datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S')
  shift_date = getShiftdate(end_time_tmp)

  t_f = str(calendar_date)+" "+str(pdm_start_time)
  t_e = str(calendar_date)+" "+str(pdm_end_time)

  start_time = pdm_start_time
  end_time = pdm_end_time

  reason_mapped = 0; # This is the default value for the parameter
  is_split = 0; # This is the default value for the parameter
  shift_id = getShiftid(shiftTimings,shift_list,start_time)

  # Find the shift starting hours
  for d in shiftTimings:
    d = str(d).split(":")
    x = str(pdm_start_time).split(":")
    d = int(int(24 if int(d[0])==0 else d[0]))*3600+int(d[1])*60+int(d[2])
    x = int(int(24 if int(x[0])==0 else x[0]))*3600+int(x[1])*60+int(x[2])
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
      sql_query2 = "SELECT * FROM `pdm_events` WHERE `machine_id`= %s and `shift_date`<=%s ORDER BY r_no DESC LIMIT 1"
      cursor.execute(sql_query2,(machine_id,shift_date,))
      previous_data = cursor.fetchone()

      if previous_data is not None:
        previous_start = previous_data[9]
        previous_end = previous_data[10]
        previous_duration = previous_data[13]
        previous_rno = previous_data[0]
        previous_event_id = previous_data[1]
        shot_count = previous_data[11]

        x_date=str(previous_data[2]).split("-")
        y_date=str(end_date).split("-")

        temp_start = str(previous_start).split(":")
        temp_end = str(pdm_start_time).split(":")

        a_t = datetime.datetime(int(x_date[0]), int(x_date[1]), int(x_date[2]), int(temp_start[0]), int(temp_start[1]), int(temp_start[2]))
        b_t = datetime.datetime(int(y_date[0]), int(y_date[1]), int(y_date[2]), int(temp_end[0]), int(temp_end[1]), int(temp_end[2]))
        c_t = b_t-a_t
        temp_duration = ((-1*c_t.total_seconds()) if c_t.total_seconds()<0 else c_t.total_seconds())
        
        temp_min = int(temp_duration/60)
        temp_sec = int(temp_duration%60)
        pre_duration = str(temp_min)+"."+str(temp_sec).zfill(2)

        # Update the previous shift end Record......
        sql_query1 = "UPDATE `pdm_events` SET `shot_count`=%s,`end_time`=%s,`duration`=%s WHERE `r_no`=%s"
        cursor.execute(sql_query1,(shot_count,start_time,pre_duration,previous_rno,))
        db_instance.commit()

        # # Update it in Reason mapping Table
        if previous_data[12] != "Active":
          child_duration = get_child_duration(previous_event_id,end_date,end_time)
          sql_query2 = "UPDATE `pdm_downtime_reason_mapping` SET `end_time`=%s,`split_duration`=%s WHERE `machine_event_id`=%s ORDER BY `start_time` DESC LIMIT 1"
          cursor.execute(sql_query2,(start_time,child_duration,previous_event_id,))
          db_instance.commit()

        # # Insert the Next shift first record
        end_time_t = str(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"))+" "+str(end_time)
        shift_date = getShiftdate((datetime.datetime.strptime(str(end_time_t), '%Y-%m-%d %H:%M:%S')))
        timestamp_t = datetime.datetime.strptime(str(str(calendar_date)+" "+str(start_time)), '%Y-%m-%d %H:%M:%S')

        event = previous_data[12]
        part_id = previous_data[7]
        tool_id = previous_data[6]
        source = "Schedular"
        duration=0

        sql_query = "INSERT INTO `pdm_events`(`machine_id`, `calendar_date`, `shift_date`, `shift_id`, `start_time`, `end_time`,`shot_count`, `event`, `duration`, `reason_mapped`, `is_split`,`part_id`,`tool_id`, `source` , `timestamp`) VALUES(%s ,%s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s , %s, %s ,%s, %s ,%s)"
        val = (machine_id , calendar_date , shift_date , shift_id , start_time , end_time ,shot_count , event,duration , "0" , "0" ,part_id, tool_id, source, timestamp_t)
        cursor.execute(sql_query,val)
        db_instance.commit()
  print("Data stored in downtime event tables......")
  return


#<------------------------------------- process data ------------------------------------------------>

def process_data(shiftTimings,machine,shift_list, duration_start = 0, duration_end = 0):

  s_hrs = now.strftime("%H")
  e_hrs = str(int(24 if now.strftime("%H")=="00" else now.strftime("%H"))+1).zfill(2)

  if(duration_end != 0):
    e_hrs = s_hrs

  pdm_start_time = str(s_hrs).zfill(2)+":"+str(duration_start).zfill(2)+":"+"00"
  pdm_end_time = str(e_hrs).zfill(2)+":"+str(duration_end).zfill(2)+":"+"00"   
 
  process_data_pdm_info(machine,pdm_start_time,pdm_end_time,shiftTimings,shift_list)

  process_data_pdm_downtime(machine,shiftTimings,pdm_start_time,shift_list,pdm_end_time)
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
  machines = getMachineinfo(database_connection().connect_sql())
  shift_hours = [int(i.strftime("%H")) for i in shiftTimings]
  shift_min = [int(i.strftime("%M")) for i in shiftTimings]
  shift_list = getShiftList(shiftTimings)

  #<---------------------- Loop break daywise ------------------------->
  while(True):
    now = datetime.datetime.now(pytz.timezone('Asia/Kolkata')) # Take current time to check the current shift hours
    break_loop = 0
    try:
      if(int(now.strftime("%M")) ==0): # you can change time here to run the code
        logger.info("connected")
        logger.info(now)
    #<------------------- Machine wise data processing ------------------->
        for count,value in enumerate(machines):

          machine = value[0]
          if(int(shiftTimings[0].strftime("%M")) != 0 and (int(now.strftime("%H"))) in shift_hours):

            break_loop = shift_hours.index((int(now.strftime("%H")))-1)

            for i in range(2):
              if(i==0):
                print("1")
                process_data(shiftTimings,machine,shift_list, duration_start = 0, duration_end = shiftTimings[0].strftime("%M"))
              else:
                print("2")
                process_data(shiftTimings,machine,shift_list, duration_start = shiftTimings[0].strftime("%M"), duration_end = 0)
          else:
            print("trigger")
            process_data(shiftTimings,machine,shift_list, duration_start = 0, duration_end = 0)

          # print(f'{0} completed',(machine))

  #<------------------------------------- end of processing hourly ------------------------------------------------>

        time.sleep(65)

      # This is the line which help to break the loop to retrive machine,shift data again
      if(break_loop>len(shiftTimings)-1):
        break
    except BaseException as err:
      logger.info(err)
      logger.info(traceback.format_exc()) 
      time.sleep(60)             
      


#<------------------------------------------------------ end ---------------------------------------------------->



