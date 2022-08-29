# Write your code here
import csv
import pandas as pd
import re
import sqlite3
import json
from lxml import etree
#FUNCTIONS
def remove_checked(string):
    pos = string.find("[")
    final_string = string[:pos]
    return final_string
def extract_integer(string):
    final_string = ""
    for i in range (len(string)):
        if string[i].isdigit() == True :
            final_string += string[i]
        else:
            pass
    if final_string == string:
        return final_string, False
    elif final_string != string and bool(re.search(r'\d', string)):
        return final_string, True
    else:
        return final_string, False

def correct_data(csv_file):
    list1 = []
    with open(csv_file, "r") as file:
        file_reader = csv.reader(file, delimiter=",")
        for line in file_reader:
            list1.append(line)
        file.close()
    count = 0
    for element in list1:
        for i in range(len(element)):
            element[i], cell_corrected = extract_integer(element[i])
            if cell_corrected:
                count += 1
    with open(f"{csv_file[:-4]}[CHECKED].csv", "w", encoding="utf-8") as w_file:
        file_writer = csv.writer(w_file, delimiter=",", lineterminator="\n")
        for i in range(1,len(list1)):
            file_writer.writerow(list1[i])
        return w_file, count

def SQL_to_json(db_file):
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    vehicle_dict = {"convoy":[]}
    table = cursor.execute("""SELECT * FROM convoy;""")
    connection.commit()
    rows = table.fetchall()

    temporary_list = []
    count = 0
    for row in rows:
        temporary_dict = {"vehicle_id":row[0], "engine_capacity":row[1], "fuel_consumption":row[2], "maximum_load":row[3]}
        temporary_list.append(temporary_dict)
        count += 1
    resulting_dict = vehicle_dict.update({"convoy":temporary_list})
    with open(f"{db_file[:-5]}.json", "w") as j_file:
        json.dump(vehicle_dict, j_file)


    if count == 1:
        print(f"1 vehicle was saved into {db_file[:-5]}.json")
    else:
        print(f"{count} vehicles were saved into {db_file[:-5]}.json")
    connection.close()


def SQL_to_XML(db_file):
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    vehicle_xml_string = "<convoy>"
    table = cursor.execute("""SELECT * FROM convoy;""")
    connection.commit()
    rows = table.fetchall()
    temporary_string = ""
    count = 0
    for row in rows:
        temporary_string += "<vehicle>"
        temporary_string += f"<vehicle_id>{row[0]}</vehicle_id>"
        temporary_string += f"<engine_capacity>{row[1]}</engine_capacity>"
        temporary_string += f"<fuel_consumption>{row[2]}</fuel_consumption>"
        temporary_string += f"<maximum_load>{row[3]}</maximum_load>"
        temporary_string += "</vehicle>"
        count += 1
    vehicle_xml_string += temporary_string + "</convoy>"
    root = etree.fromstring(vehicle_xml_string)
    tree = etree.ElementTree(root)
    tree.write(f"{db_file[:-5]}.xml")
    if count == 1:
        print(f"1 vehicle was saved into {db_file[:-5]}.xml")
    else:
        print(f"{count} vehicles were saved into {db_file[:-5]}.xml")

def pitstop_count(engine_capacity, fuel_consumption):
    route = 450
    burned_fuel = (route * fuel_consumption / 100)
    number_pitstop = (burned_fuel // engine_capacity)
    return number_pitstop, burned_fuel


def score_get(engine_capacity, fuel_consumption, maximum_load):
    score = 0
    pitstops, liters_consumed = pitstop_count(engine_capacity, fuel_consumption)
    if pitstops >= 2:
        pass
    elif pitstops == 1:
        score += 1
    else:
        score += 2

    if liters_consumed <= 230:
        score += 2
    else:
        score += 1

    if maximum_load >= 20:
        score += 2
    else:
        pass

    return score


#MAIN PROGRAM
print("Input file name")
file_name = input()
if file_name[-5:] == ".xlsx":
    my_df = pd.read_excel(r'{}'.format(file_name), sheet_name='Vehicles', dtype=str)
    file_name2 = file_name[:-5]
    my_df.to_csv(f'{file_name2}.csv', index = None)
    shape = my_df.shape
    rows = shape[0]
    if rows == 1:
        print(f"1 line was added to {file_name2}.csv")
    else:
        print(f"{rows} lines were added to {file_name2}.csv")
    correct_file, cell_count = correct_data(f"{file_name2}.csv")
    if cell_count == 1:
        print(f"1 cell was corrected in {file_name2}[CHECKED].csv")
    else:
        print(f"{cell_count} cells were corrected in {file_name2}[CHECKED].csv")
    file_name = file_name2
elif file_name.find("[CHECKED].csv")!=(-1):
    file_name = remove_checked(file_name)
elif file_name.find(".s3db")!=(-1):
    pass
else:
    correct_file, cell_count = correct_data(file_name)
    if cell_count == 1:
        print(f"1 cell was corrected in {file_name[:-4]}[CHECKED].csv")
    else:
        print(f"{cell_count} cells were corrected in {file_name[:-4]}[CHECKED].csv")
    file_name = file_name[:-4]


if file_name.find(".s3db")!=(-1):
    pass
else:
    conn = sqlite3.connect(f'{file_name}.s3db')
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS convoy (vehicle_id INT PRIMARY KEY,
                        engine_capacity INT NOT NULL,
                        fuel_consumption INT NOT NULL,
                        maximum_load INT NOT NULL,
                        score INT NOT NULL);""")
    with open(f"{file_name}[CHECKED].csv", "r") as c_file:
        no_records = 0
        f_reader = csv.reader(c_file, delimiter = ",")
        for line in f_reader:
            start_execute =True
            for item in line:
                if item.isdigit() == False:
                        start_execute = False
                        break
            if start_execute:
                line = list(line)
                score = score_get(int(line[1]), int(line[2]), int(line[3]))
                line.append(score)
                cursor.execute(f"""INSERT or IGNORE INTO convoy VALUES {tuple(line)}""")
                no_records += 1
                conn.commit()
            else:
                pass


    if no_records == 1:
        print(f"1 record was inserted was inserted into {file_name}.s3db")
    else:
        print(f"{no_records} records were inserted into {file_name}.s3db")
    file_name = f"{file_name}.s3db"
    conn.close()

conn = sqlite3.connect(f'{file_name}')
cursor = conn.cursor()
table = cursor.execute("""SELECT * FROM convoy""")
rows = cursor.fetchall()
vehicle_dict = {"convoy":[]}
JSON_count = 0
vehicle_xml_string = "<convoy>"
XML_count = 0
temporary_string = ""
temporary_list = []
for row in rows:
    if row[4] > 3:
        temporary_dict = {"vehicle_id":row[0], "engine_capacity":row[1], "fuel_consumption":row[2], "maximum_load":row[3]}
        temporary_list.append(temporary_dict)
        JSON_count += 1

    else:
        temporary_string += "<vehicle>"
        temporary_string += f"<vehicle_id>{row[0]}</vehicle_id>"
        temporary_string += f"<engine_capacity>{row[1]}</engine_capacity>"
        temporary_string += f"<fuel_consumption>{row[2]}</fuel_consumption>"
        temporary_string += f"<maximum_load>{row[3]}</maximum_load>"
        temporary_string += "</vehicle>"
        XML_count += 1

resulting_dict = vehicle_dict.update({"convoy":temporary_list})
with open(f"{file_name[:-5]}.json", "w") as j_file:
    json.dump(vehicle_dict, j_file)

if JSON_count == 1:
    print(f"1 vehicle was saved into {file_name[:-5]}.json")
else:

    print(f"{JSON_count} vehicles were saved into {file_name[:-5]}.json")
if XML_count == 1:
    print(f"1 vehicle was saved into {file_name[:-5]}.xml")
else:
    print(f"{XML_count} vehicles were saved into {file_name[:-5]}.xml")
vehicle_xml_string += temporary_string + "</convoy>"
root = etree.fromstring(vehicle_xml_string)
tree = etree.ElementTree(root)
tree.write(f"{file_name[:-5]}.xml",  method='html')
conn.commit()
conn.close()
