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

# 主程序
def main():
    # 数据库连接信息
    db_configs = [
        {"uri": "bolt://43.153.97.134:7687", "user": "neo4j", "pwd": "11111111"},
        {"uri": "bolt://43.128.99.239:7687", "user": "neo4j", "pwd": "11111111"},
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
        merged_df, _ = merge_multiple_dataframes(dataframes)
        analyze_and_plot_cumulative(merged_df, "Merged Database Analysis")

        # 所有数据库均包含的节点个数
        common_nodes_count = compare_multiple_dataframes(dataframes)
        print(f"same nodes counts: {common_nodes_count}")

        # 合并所有数据库之后的节点个数及情况，导入csv文件
        merged_df, merged_count = merge_multiple_dataframes(dataframes)
        print(f"merged nodes counts: {merged_count}")
        # save_dataframe_to_csv(merged_df, 'merged_data.csv')

if __name__ == "__main__":
    main()