import pandas as pd

trace_downstream_df = pd.read_csv('MSCallGraph_0.csv')
microservice_node_df = pd.read_csv('MSResource_0.csv')
node_cpu_df = pd.read_csv('Node_0.csv')

trace_downstream_df['timestamp'] = round(trace_downstream_df['timestamp'] / 30000) * 30000


trace_microservice_node_df = pd.merge(
    trace_downstream_df, microservice_node_df,
    left_on=['dm', 'timestamp'],
    right_on=['msname', 'timestamp']
)


trace_microservice_node_cpu_df = pd.merge(
    trace_microservice_node_df, node_cpu_df,
    left_on=['nodeid', 'timestamp'],
    right_on=['nodeid', 'timestamp']
)


high_cpu_usage_df = trace_microservice_node_cpu_df[trace_microservice_node_cpu_df['node_cpu_usage'] > 0.8]

all_num = trace_downstream_df['traceid'].nunique()
high_num = high_cpu_usage_df['traceid'].nunique()

print(f'Number of traces : {all_num}')
print(f'Number of traces with at least one downstream microservice node CPU usage > 80%: {high_num}')

column_selected = ['traceid','dm','timestamp']

my_df = high_cpu_usage_df[column_selected]
my_df = my_df.drop_duplicates()

grouped = my_df.groupby(['dm', 'timestamp'])
my_group = my_df.groupby(['traceid','timestamp'])

competing_traces_set = set()
my_set = set()
maxnum = 0
for name, group in grouped:
    if len(group) > 2:
        competing_traces_set.update(group['traceid'].unique())
        maxnum = max(maxnum,len(group))
for name,group in my_group:
    traceid,timestamp = name
    if len(group) > 1:
        my_set.add(traceid)
print("max trace use one overload service at one time:",maxnum)
competing_traces_set = competing_traces_set & my_set     
unique_competing_traces_count = len(competing_traces_set)

print(f'Number of traces in topfull definition: {unique_competing_traces_count}')
