import csv
import pandas as pd 

df = pd.read_csv('report.csv')

bidimensional_array = []
with open('report.csv', newline='\n') as f:
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        bidimensional_array.append([str(x) for x in row])


x = int()
parents=[]
groups = []
clients = []

pcg_dict = {}
master=[]
allNumbers = []
z = 0
j = 0
y = 0
p = 0
today = pd.to_datetime('today').normalize()
print(today)

# Creates a list of dictionaries that have a key of the parent task and a value of all tasks (parent and child) with a single value per key
for x in bidimensional_array:


    if x[9] == "Implementation File Receipt":
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
                master.append({parents[(len(parents)-1)]: (x[0]+("|"))})

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
print(npd_df)

# create a dict oF equal length to parents that returns to client and group name in the same order
for i in parents:
    pcg_dict[i]= clients[j] + "|" + groups[j]
    j=j+1

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


# combine parent:subtask and parent:client, group dictionaries based on matching parent keys
final = {key: pcg_dict[key] + "|" + res[key] for key in pcg_dict}
# print(final)

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

res_df1["Status"] = status
res_df1["Tracker"] = tracker
res_df1["Created_Date"] = created
res_df1["Age"] = age
res_df1=res_df1.drop(columns=['IsIn'])

# Create csv (pipe delimited)
res_df1.to_csv("out.csv", index=False)


# NEXT STEPS: ADD AGE