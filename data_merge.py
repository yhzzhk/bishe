from neo4j import GraphDatabase
import pandas as pd
import matplotlib.pyplot as plt


# Neo4j数据库连接
class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__password = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


# 连接到数据库并执行查询的函数
def query_database(connection, query):
    return connection.query(query)

# 将查询结果转换为DataFrame的函数
def convert_to_dataframe(result):
    return pd.DataFrame([dict(record['n']) for record in result])

# 查询所有数据库中都有的节点
def compare_multiple_dataframes(dfs):
    # 比较所有DataFrame，找出所有数据库中都有的节点
    common_nodes = dfs[0]
    for df in dfs[1:]:
        common_nodes = pd.merge(common_nodes, df, on='id')
    return common_nodes.shape[0]

# 合并所有数据库中的节点发现结果
def merge_multiple_dataframes(dfs):
    merged_df = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
    return merged_df, merged_df.shape[0]

# 比较任意两个或三个数据库之间共同的节点
def compare_specific_combinations(dfs, combination):
    common_nodes = dfs[combination[0] - 1]
    for index in combination[1:]:
        common_nodes = pd.merge(common_nodes, dfs[index - 1], on='id')
    return common_nodes.shape[0]

# 保存DataFrame到CSV文件的函数
def save_dataframe_to_csv(df, filename):
    df.to_csv(filename, index=False)

def analyze_and_plot_cumulative(df, title):
    # 确保last_time是datetime类型
    df['last_time'] = pd.to_datetime(df['last_time'])

    # 按时间排序
    df = df.sort_values('last_time')

    # 统计各个指标
    total_counts = df['last_time'].value_counts().sort_index().cumsum()
    inbound_df = df[df['is_inbound'] == True]
    inbound_counts = inbound_df['last_time'].value_counts().sort_index().cumsum()
    dyndial_df = df[df['is_dyndial'] == True]
    dyndial_counts = dyndial_df['last_time'].value_counts().sort_index().cumsum()

    # 绘图
    plt.figure(figsize=(10, 6))
    total_counts.plot(label='Total Nodes')
    inbound_counts.plot(label='Inbound Nodes')
    dyndial_counts.plot(label='Dyndial Nodes')
    plt.xlabel('Last Time')
    plt.ylabel('Cumulative Node Count')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()

    # 文字描述统计
    total_last = total_counts.iloc[-1]
    inbound_last_time = inbound_df['last_time'].max() if not inbound_df.empty else None
    inbound_last = inbound_counts[inbound_last_time] if inbound_last_time else 0
    dyndial_last_time = dyndial_df['last_time'].max() if not dyndial_df.empty else None
    dyndial_last = dyndial_counts[dyndial_last_time] if dyndial_last_time else 0

    print(f"{title}\nTotal nodes counts: {total_last}\n"
          f"Inbound nodes counts (at last inbound time): {inbound_last}\n"
          f"Dyndial nodes counts (at last dyndial time): {dyndial_last}\n")

# 读取etherscan文件
def read_ips_from_txt_etherscan(filename):
    unique_ips = set()
    try:
        with open(filename, 'r') as file:
            for line in file:
                # 提取每行中第一个空格之前的内容作为IP地址
                ip = line.split()[0]  # 使用空格分割并获取第一个元素
                unique_ips.add(ip)
        return list(unique_ips)
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# 读取ethernodes文件
def read_ips_from_txt_ethernodes(filename):
    unique_ips = set()
    try:
        with open(filename, 'r') as file:
            for line in file:
                # 分割每行并提取IP地址
                parts = line.split(',')
                if len(parts) > 1:
                    ip = parts[1]  # 第二个元素是IP地址
                    unique_ips.add(ip)
        return list(unique_ips)
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# 比较两组IP地址
def compare_ips(txt_ips, merged_ips):
    # 找出相同的IP地址
    count=0
    for ip in merged_ips:
        if ip in txt_ips:
            count=count+1
    # 计算各自独有的IP地址数量
    unique_to_txt = len(txt_ips) - count
    unique_to_merged = len(merged_ips) - count
    return count, unique_to_txt, unique_to_merged

# 主程序
def main():
    # 数据库连接信息
    db_configs = [
        {"uri": "bolt://43.153.97.134:7687", "user": "neo4j", "pwd": "11111111"},
        {"uri": "bolt://43.128.99.239:7687", "user": "neo4j", "pwd": "11111111"},
        {"uri": "bolt://162.62.224.119:7687", "user": "neo4j", "pwd": "11111111"},
        # 可以在此处添加更多数据库配置
    ]

    # 存储所有DataFrame的列表
    dataframes = []

    # 循环处理每个数据库
    for config in db_configs:
        conn = Neo4jConnection(uri=config["uri"], user=config["user"], pwd=config["pwd"])
        result = query_database(conn, "match (n) where n.is_eth_handshake_complete=true return n")
        df = convert_to_dataframe(result)
        dataframes.append(df)
        conn.close()
    
    # 分析和绘图
    for i, df in enumerate(dataframes, start=1):
        analyze_and_plot_cumulative(df, f"Database {i} Analysis")
    

    # 比较和合并多个DataFrame, 分析合并后的dataframe
    if len(dataframes) > 1:

        # 对合并后的dataframe做画图统计分析
        merged_df, merged_count = merge_multiple_dataframes(dataframes)
        analyze_and_plot_cumulative(merged_df, "Merged Database Analysis")
        # print(merged_df[['ip']])
        print("----------------------------------------------")
        # 所有数据库均包含的节点个数
        # 比较不同组合的数据库
        combinations = [(1, 2), (2, 3), (1, 3), (1, 2, 3)]
        for combo in combinations:
            common_count = compare_specific_combinations(dataframes, combo)
            print(f"Common nodes for databases {combo}: {common_count}")

        # 合并所有数据库之后的节点个数及情况，导入csv文件
        print(f"merged nodes counts: {merged_count}")
        # save_dataframe_to_csv(merged_df, 'merged_data.csv')

        print("------------------------------------------------")
        txt_ips_ethernodes = read_ips_from_txt_ethernodes('results.txt') # 读取ethernodes的txt文件中的IP地址
        txt_ips_etherscan = read_ips_from_txt_etherscan('0123.txt') # 读取ethernodes的txt文件中的IP地址
        merged_ips = merged_df['ip'].drop_duplicates() # 提取并去重IP地址
        common_count_ethnodes, unique_to_txt_ethnodes, unique_to_merged_ethnodes = compare_ips(txt_ips_ethernodes, merged_ips) # 比较ethernodes
        common_count_etherscan, unique_to_txt_etherscan, unique_to_merged_etherscan = compare_ips(txt_ips_etherscan, merged_ips) # 比较etherscan
        # 输出结果
        print("ethernodes compare results:")
        print(f"common ips: {common_count_ethnodes}")
        print(f"ethernodes_results: {len(txt_ips_ethernodes)}")
        print(f"merged_results: {len(merged_ips)}")
        print(f"ethernodes_unique: {unique_to_txt_ethnodes}")
        print(f"merged_unique: {unique_to_merged_ethnodes}")

        print("etherscan compare results:")
        print(f"common ips: {common_count_etherscan}")
        print(f"etherscan_results: {len(txt_ips_etherscan)}")
        print(f"merged_results: {len(merged_ips)}")
        print(f"etherscan_unique: {unique_to_txt_etherscan}")
        print(f"merged_unique: {unique_to_merged_etherscan}")

        
    

if __name__ == "__main__":
    main()