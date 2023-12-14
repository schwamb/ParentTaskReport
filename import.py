import csv
import pandas as pd 
df = pd.read_csv('report.csv')

bidimensional_array = []
with open('report.csv', newline='\n') as f:
    reader = csv.reader(f, delimiter=',')
    # next(reader) 
    for row in reader:
        bidimensional_array.append([str(x) for x in row])


x = int()
parents=[]
groups = []
clients = []
tracker = []
pcg_dict = {}
master=[]
allNumbers = []
z = 0
j = 0
y = 0
p = 0


# print(allParentData)

# Creates a list of dictionaries that have a key of the parent task and a value of all tasks (parent and child) with a single value per key
for x in bidimensional_array:
    allNumbers.append(x[0])  
    # Need to create logic that only creates dictionaries when there are IFRs and when parent isn't blank
    z=z+1
    if x[9] == "Implementation File Receipt":
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

rawData = pd.DataFrame(bidimensional_array)
pdata = (rawData[rawData[0].isin(parents)])
# print(pdata)
# IF PARENTS HAS A VALUE THAT IS NOT IN PDATA, THEN THAT VALUE SHOULD BE DROPPED FROM PARENTS, BUT THIS SHOULD HAPPEN AFTER PARENTS, GROUPS AND CLIENTS HAVE BEEN COMBINED
numericPData = pd.to_numeric(pdata[0])
npd_df = pd.DataFrame(numericPData)
npd_df["Tracker"]=pdata[1]

npdcol = ["Parent_Task","Tracker"]
npd_df.columns= npdcol
# print(npd_df)





# parents_df = pd.DataFrame(parents)
# numericParents = pd.to_numeric(parents_df[0])
# np_df = pd.DataFrame(numericParents).sort_values(0)
# compare_all = pd.DataFrame(allParentData[0]).reset_index()
# print(npd_df)

# # parents2=(rawData[rawData[0].isin(rawData[11])])



# # NEXT STEP: Add parent task tracker type to dict
# #     if x[9] != "Implementation File Receipt":
# #             if x[11] not in parents:
# #                 tracker.append(x[9])
# # print(len(tracker))
# # print(len(parents))

# for x in allNumbers:
#     if parents[y] in allNumbers:
#     #     tracker.append(x[9])
#         print("yes")
#     # print(allNumbers)
#         # print(parents[y])
#         y=y+1


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

# j = 0
# for i in holder:
#     if holder[j] == bidimensional_array[0]:
#         print(holder[j])


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
# print(res_df1)

# res_df.assign(InDf2=res_df.Parent_Task.isin(npd_df.IDs).astype(int))

res_df1 = res_df1.apply(pd.to_numeric, errors = "ignore")
res_df1["IsIn"] = res_df1["Parent_Task"].isin(npd_df["Parent_Task"])
res_df1=res_df1[res_df1.IsIn]
npd_df = pd.DataFrame(npd_df).sort_values("Parent_Task")
print(npd_df["Tracker"])
res_df1 = pd.DataFrame(res_df1).sort_values("Parent_Task")
# res_df1=pd.DataFrame.to_string(res_df1)

tracker = npd_df["Tracker"].tolist()

res_df1["Tracker"] = tracker
res_df1=res_df1.drop(columns=['IsIn'])


# print(res_df1)



# print(res_df1.dtypes)


print(res_df1)
res_df1.to_csv("out.csv", index=False)


