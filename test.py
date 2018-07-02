import pdb
import psycopg2
import datetime
import numpy
import os, sys
import base64

class textFile:
    def __init__(self, textfile_path):
        self.parse_textfile(textfile_path)
        self.classify_textfile()
        self.treat_textfile()
        self.select_textfile()

    def parse_textfile(self, textfile_path):
        text_file = open(textfile_path, "r")
        self.lines = text_file.read().split('","')
        self.lines = self.lines[1:]
        self.lines[-1] = self.lines[-1][:-2]
        print(self.lines[-1])
        self.elements = len(self.lines)

    def select_textfile(self):
            for index, line in enumerate(self.lines):
                for index, line2 in enumerate(self.lines):
                    if index == 0 and line == line2:
                        pass
# correct it & make it choose the file with least nulls

     #       if self.label in ('customer_1', 'customer_2'):
      #         textfile = numpy.select([self.lines[0]==self.lines[0]],[min(None)])

    def treat_textfile(self):
        for index, line in enumerate(self.lines):
          if line == '':
            self.lines[index] = None

        # self.lines = [None if line == '' else line for line in self.lines]

    def classify_textfile(self):
        if self.elements>=33 and '/' in self.lines[2]:
         self.label="customer_1"
         for index, lin in enumerate(self.lines):
             if index == 13 and lin == '':
                 self.lines[13] = 'SVTUI178'
             elif index == 14 and lin == '':
                self.lines[14] = 'unknown1'
        elif self.elements>=33 and '/' not in self.lines[2]:
         self.label="customer_2"
         for index2, lin2 in enumerate(self.lines):
             if index2 == 11 and lin2 == '':
                 self.lines[11] = 'SVTUI178'
             elif index2 == 12 and lin2 == '':
                self.lines[12] = 'unknown1'
        elif self.elements==8:
         self.label="national_id_card"
         for index, lin in enumerate(self.lines):
             if index == 1 and lin == '':
                 self.lines[1] = ' '
        else:
         self.label="leader_declaration"

    def decode_textfile(self): #not sure if this is correct
        for index, line in enumerate(self.lines):
          if index == 0 and 'base64' in line:
              image=write(base64.decodestring(self.lines[index]))
              image.save("leader_declaration_images/"+self.lines[0])

# Get all filenames in a directory (import dir, sys)
# loop over all filenames, make textfile objects, and put them in list

textfiles = [] # The list where we put the textfile objects

textfile_paths = [] # get all file paths in this list
for folder, subs, files in os.walk("C:\\Users\\Catalyst\\AppData\\Local\\Programs\\Python\\Python36-32\\Test"):
  for filename in files:
    textfile_paths.append(os.path.abspath(os.path.join(folder, filename)))

for textfile_path in textfile_paths:
    textfile = textFile(textfile_path) # Make a new textFile object
    textfiles.append(textfile) # Add the textFile object to the end of the list

conn=psycopg2.connect(host="solarworks-appsheet-dev.cpikw3v6qghd.eu-central-1.rds.amazonaws.com",database="appsheet", user="appsheet", password="B4752o349fVkiPdj") #connect to database
cur=conn.cursor() #create a new cursor

for textfile in textfiles:
#    sorted(textfiles, key=attrgetter(textfile.label))
#       textfile.lines = ['SVTUI178' if line[14] == 'Null' else line[14] for line in textfile.lines]
    if textfile.label=="customer_1":
        cur.execute("""
                 INSERT INTO customer (id, primavera_id, created_at, created_by, responsible_agent_id, first_name,
                  last_name, birth_date, gps, phone_number_1, phone_number_2, gender, shop_id, area_id,
                  neighbourhood_id, address, system_interested_in_id, interested_in_appliance,
                  appliance_interested_in_id, cash_or_credit, months_interested_in, power_used_for, comment, 
                  credit_check_approved, customer_info_complete, is_preliminary_customer, is_customer, is_shown, follow_up_date)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '2018/07/01');
                 """,
               #    ((textfile.lines[:28])))
                    ((textfile.lines[:21] + textfile.lines[23:29] + [textfile.lines[31]]))) #combine elements into a single array

    elif textfile.label=="customer_2":
        cur.execute("""
        	 	INSERT INTO customer (id, primavera_id, first_name, last_name, birth_date, gps, phone_number_1,
        	 	 phone_number_2,customer_photo, gender, shop_id, area_id, neighbourhood_id, address, system_interested_in_id,
        	 	 appliance_interested_in_id, power_used_for, cash_or_credit, months_interested_in, follow_up_date,
        	 	 next_step, comment, credit_check_approved, customer_info_complete, is_preliminary_customer, is_customer,
        	 	 is_shown, created_at, created_by, responsible_agent_id, interested_in_appliance)
        		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
     	        """,
         			((textfile.lines[:31])))
    elif textfile.label=="national_id_card":
	    cur.execute("""
		        INSERT INTO national_id_card (id, picture_front, customer_id, type, identification_number, valid_until, nuit, picture_back)
	        	VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
		        """,
		            ((textfile.lines[:8])))
    elif textfile.label=="leader_declaration":
	    cur.execute("""
    	        INSERT INTO leader_declaration (id, customer_id, father_name, mother_name, bi_nr, civil_id, id_at, city_of_origin, birth_date, neighbourhood_id, quar, phone_number, house_number, declaration, signed_at, signed_date, leader_sign)
    	        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            	""",
		            ((textfile.lines[:17])))


conn.commit()



#pdb.set_trace()

# if len(lines)>=33:
# 	def is_date(string):
# 		try:
# 			date_time = parse(lines[4])
#             #check if the 4th argument is a date
#             cur.execute("""
#             INSERT INTO customer (id, primavera_id, created_at, created_by, responsible_agent_id, first_name, last_name, birth_date, gps, phone_number_1, phone_number_2, gender, shop_id, area_id, neighbourhood_id, address, system_interested_in_id, interested_in_appliance, appliance_interested_in_id, cash_or_credit, months_interested_in, power_used_for, comment, credit_check_approved, customer_info_complete, is_preliminary_customer, is_customer, is_shown)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#             """,
#             (numpy.concatenate(lines[1:22], lines[25:34], lines[36]))) #combine elements into a single array
# 		except ValueError:
# 				cur.execute("""
# 	        	INSERT INTO customer (id, primavera_id, first_name, last_name, birth_date, gps, phone_number_1, phone_number_2,customer_photo, gender, shop_id, area_id, neighbourhood_id, address, system_interested_in_id, appliance_interested_in_id, power_used_for, cash_or_credit, months_interested_in, follow_up_date, next_step, comment, credit_check_approved, customer_info_complete, is_preliminary_customer, is_customer, is_shown, created_at, created_by, responsible_agent_id, interested_in_appliance)
# 	    		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
# 	        	""",
# 				((lines[1:32])))

#
# else:


cur.close()
#
#
#
# except (Exception, psycopg2.DatabaseError) as error: print(error)
# finally: if conn is not None: conn.close()
#
# text_file.close()
