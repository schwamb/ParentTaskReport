import csv
import pandas as pd 
from datetime import datetime, date, timedelta

# from datetime import date
# import holidays as pyholidays

df = pd.read_csv('vcrreport.csv')

bidimensional_array = []
with open('vcrreport.csv', encoding='windows-1252', newline='\n') as f:
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        bidimensional_array.append([str(x) for x in row])

# Sort by parent task IDs
bidimensional_array.sort(key=lambda x:x[11], reverse=True)


x = int()
parents=[]
groups = []
clients = []
links = []
ifr_status = []
ngo_list = []
open_age = []
closed_age = []
closed_date = []
date_format = '%Y-%m-%d %I:%M %p'
now = datetime.now()
today = now.strftime(date_format)


pcg_dict = {}
master=[]
allNumbers = []

z = 0
j = 0
y = 0
p = 0
today = pd.to_datetime('today').normalize()
# Permanently changes the pandas settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
 
# All dataframes hereafter reflect these changes.

# Creates a list of dictionaries that have a key of the parent task and a value of all tasks (parent and child) with a single value per key
for x in bidimensional_array:
    if x[9] == "Vendor Change Request":
        if x[1] == "Closed":
            date_obj1 = datetime.strptime(x[8], date_format)
            closed_obj = datetime.strptime(x[13], date_format)
            closed_age.append(date_obj1)
            closed_date.append(closed_obj)
        if x[1] != "Closed":
            ngo_list.append(x[0])
            date_obj2 = datetime.strptime(x[8], date_format)
            open_age.append(date_obj2)
            
    if x[9] == "Implementation File Receipt":
        ifr_status.append(x[0] + " " + x[12])
        # Creates dictionaries when there are IFRs and when parent isn't blank
        if x[11] != "":
            #this creates a list of unique parents tasks to be used as the keys in our dictionary
            if x[11] not in parents:
                parents.append(x[11])
                groups.append(x[10])
                clients.append(x[3])
            # now we need to get the values and input them into the appropriate dict
            # if value in x11 matches value in parents list then 
            if x[11] == parents[(len(parents))-1]:
                master.append({parents[(len(parents)-1)]: (x[0]+("|") + x[12] + "|")})

# Display the count of open NGOs
print("Total Open NGO: " + str(len(ngo_list)))


#Calculating the age of Closed NGOs from creation to closed date in business days converted to weeks
results1=[]
yz = 0
for x in closed_age:
    dates = (closed_age[yz] + timedelta(idx + 1)
            for idx in range((closed_date[yz] - closed_age[yz]).days))
    
# summing all weekdays
    result1 = sum(1 for day in dates if day.weekday() < 5)
    results1.append(result1)
    # print(open_age[xy])
    yz = yz + 1
avg_closed = round(((sum(results1)/len(results1))/5),2)
print("Average Weeks Open to Production: " + str(avg_closed) + " weeks.")


#Calculating the age of Open NGOs from creation to today in business days converted to weeks
results=[]
xy = 0
# generating dates
for x in open_age:
    dates = (open_age[xy] + timedelta(idx + 1)
            for idx in range((today - open_age[xy]).days))
    
# summing all weekdays
    result = sum(1 for day in dates if day.weekday() < 5)
    results.append(result)
    # print(open_age[xy])
    xy = xy + 1
avg_open = round(((sum(results)/len(results))/5),2)
# printing
print("Average Open Aging in Weeks: " + str(avg_open) + " weeks.")




# create a dataframe of all task values in bidimensional_array, then check for all values in parents. Create a dataframe with all parent task IDs, status, created date, tracker type, and age
rawData = pd.DataFrame(bidimensional_array)
pdata = (rawData[rawData[0].isin(parents)])


numericPData = pd.to_numeric(pdata[0])
npd_df = pd.DataFrame(numericPData)
npd_df["Status"]=pdata[1]
npd_df["Created"]=pd.to_datetime(pdata[8], unit='ns')
npd_df["Tracker"]=pdata[9]
npd_df["Age"]=(today-npd_df.Created).astype('timedelta64[ns]')

npdcol = ["Parent_Task","Status","Created", "Tracker","Age"]
npd_df.columns= npdcol



# create dictionary with parent task as key and subtasks as values
res = dict()
holder = []
for dict in master:
    for list in dict:
        if list in res:
            res[list] += (dict[list])

        else:
            res[list] = dict[list]
            holder.append(list)


# create a dict of equal length to parents that returns the client and group name in the same order
for i in parents:
    pcg_dict[i]= clients[j] + "|" + groups[j]
    j=j+1

# combine parent:subtask and parent:client, group dictionaries based on matching parent keys
final = {key: pcg_dict[key] + "|" + res[key] for key in pcg_dict}


# build dataframe and transpose columns and rows into desired orientation
res_df = (pd.DataFrame.from_dict([final])).transpose()


#add headers and split
res_df1 = pd.concat([res_df[[0]], res_df[0].str.split('|', expand=True)], axis=1)
headers = ['Drop', 'Clients', 'Groups']
k=0
numbers = []

# programmatically generating headers to DF column length
for k in range(0, ((res_df1.shape[1])-2)):
    numbers.append(str(k))
    headers.append('Subtask_' + numbers[(k)])
    k=k+1

headers.remove('Subtask_0')
res_df1.columns = headers

res_df1=res_df1.drop(columns=['Drop'])
res_df1=res_df1.reset_index()
res_df1=res_df1.rename(columns={"index":"Parent_Task"})



# check if the parent task in the res_df is in the dataframe of all parent_Tasks taken from column[0] of the bidimensional array (some will not be on account of having been deleted or not returned by deermine)
# Drop the row if the parent task is not present, sort both DFs, then add the tracker to res_df
res_df1 = res_df1.apply(pd.to_numeric, errors = "ignore")
res_df1["IsIn"] = res_df1["Parent_Task"].isin(npd_df["Parent_Task"])
res_df1=res_df1[res_df1.IsIn]
npd_df = pd.DataFrame(npd_df).sort_values("Parent_Task")
res_df1 = pd.DataFrame(res_df1).sort_values("Parent_Task")

status = npd_df["Status"].tolist()
tracker = npd_df["Tracker"].tolist()
created = npd_df["Created"].tolist()
age = npd_df["Age"].tolist()
 
count_ifrs = []
count_received = []
all_files = []
h = 2
alphabet =['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','aa','ab','ac','ad','ae','af','ag','ah','ai','aj','ak','al','am','an','ao','ap','aq','ar','as','at','au','av','aw','ax','ay','az','ba','bb','bc','bd','be','bf','bg','bh','bi','bj','bk','bl','bm','bn','bo','bp','bq','br','bs','bt','bu','bv','bw','bx','by','bz','ca','cb','cc','cd','ce','cf','cg','ch','ci','cj','ck','cl','cm','cn','co','cp','cq','cr','cs','ct','cu','cv' ]
urlID = (res_df1["Parent_Task"].tolist())
s = res_df1.shape[1]

#Turn the Parent Task columns into links by prepending "https://deermine.cgt.us/issues/"
for x in urlID:
    links.append('=HYPERLINK("https://deermine.cgt.us/issues/'+ str(x) + '","' + str(x) +'")' )

    h = h + 1

res_df1["Parent_Task"]=links
res_df1["Status"] = status
res_df1["Tracker"] = tracker
res_df1["Created_Date"] = created
res_df1["Age"] = age

res_df1=res_df1.drop(columns=['IsIn'])

res_df1 = res_df1.dropna(axis=1, how='all')
t = res_df1.shape[1]

#Add excel formulas to calculate the number of IFRs with "received" or "Ready to implement" status and compare to number or IFRs, return "TRUE" if values match
k = 2
o = 2
for x in urlID:
    count_ifrs.append('=(COUNTA(D'+str(k)+':'+ alphabet[t-5]+str(k)+')/2)')
    count_received.append('=SUM(COUNTIF(D'+str(k)+':'+ alphabet[t-5]+str(k)+',{"Received","Ready to Implement"}))')
    all_files.append('=ifna(' + alphabet[t+1] + str(o) +'='+alphabet[t] + str(o) +', FALSE)')
    o = o+1
    k = k +1

res_df1["Number of IFRs"] = count_ifrs
res_df1["Number of Files Received"] = count_received
res_df1["All Files Received"] = all_files

# Create csv (pipe delimited)
res_df1.to_csv("vcrout.csv", index=False)



# Create new DF of only open NGOs from transformed DF
open_df = res_df1
closed_NGOs = open_df[(open_df['Status']) == 'Closed'].index

open_df.drop(closed_NGOs, inplace=True)

ifr_count_pre=pd.DataFrame((((open_df.count(axis=1))-10)/2), columns=["0"])

ifr_count = []
cd = 1
ifr_count=ifr_count_pre["0"].tolist()

received_count = []
file_status_list= []
xz = 0


#Look for all IFRs with a received or ready to implement status and add to list
while xz < len(open_df):
    df1 = pd.DataFrame(open_df.iloc[xz, 0:(t-1)])
    file_status_list.append(str(((df1.iloc[1:(t-1)]).values.tolist())))

    test_str = str(file_status_list[xz])
    
    # initializing substring
    value = ["Received", "Ready to Implement"]
    
    # using list comprehension + startswith()
    # All occurrences of substring in string
    received_file = [i for i in range(len(test_str)) if test_str.startswith(value[0], i)]
    ready_file = [i for i in range(len(test_str)) if test_str.startswith(value[1], i)]

#counting number of received/ready IFRs
    received_count.append(len(received_file+ready_file))

    xz = xz + 1

ef = 0
all_file_received = 0

#Count when expected IFR per row is the same as the received/ready IFR count per row and add to list. Print total number of matching.
for x in received_count:
    if float(received_count[ef]) == float(ifr_count[ef]):
        all_file_received = all_file_received +1
    ef = ef +1
print("Waiting for Sprint: " + str(all_file_received) +" (You will need to subtract the NGOs in the PSR from this number).")
