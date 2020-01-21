import datetime
import time

import numpy as np
import happybase
from elasticsearch_dsl import connections, Search
import faiss

pool = happybase.ConnectionPool(size=10, host='localhost', port=9091)
connections.create_connection(hosts=['localhost'], timeout=20)

faiss_model_path = "faiss.model"
index = faiss.read_index(faiss_model_path)
model_update_time = ""


def get_user_profile_recall(user_id, num_items):
    """
    用户偏好召回，hbase取用户小类偏好top1，2，3，es检索再根据上市时间排名
    :param user_id:
    :param num_items:
    :return: item_list
    """
    with pool.connection() as conn:
        table = conn.table('TOPIC_LIKE')
        row = table.row(user_id, columns=[b'INFO:PTY1', b'INFO:PTY2', b'INFO:PTY3'])
        conn.close()

    search_size = {b"INFO:PTY1": 0.5, b"INFO:PTY2": 0.3, b"INFO:PTY3": 0.2}
    item_list = []
    for column, pty in row.items():
        if pty == b"null":
            continue
        s = Search(index="recsys") \
            .params(size=int(search_size[column] * num_items)) \
            .filter("terms", PTY_NUM_3=[int(pty)]) \
            .sort("-update_time") \
            .source(includes=['ITEM_NUM_ID'])
        for hit in s:
            item_list.append(hit["ITEM_NUM_ID"])

    return item_list


def get_itemcf_recall(user_id, num_items):
    """
    itemcf召回，hbase key-value调用结果
    :param user_id:
    :param num_items:
    :return:
    """
    with pool.connection() as conn:
        table = conn.table('RECSYS_ITEMCF_RECOMMEND')
        row = table.row(user_id, columns=[b'INFO:RECOMMEND'])
        conn.close()
    item_list = eval(row[b'INFO:RECOMMEND'].decode())[:num_items] if row else []

    return item_list


def get_recent_similar_recall(user_id, num_items):
    """
    最近浏览相似召回，hbase查询最近浏览，faiss查询相似商品
    :param user_id:
    :param num_items:
    :return:
    """
    user_views = []
    with pool.connection() as conn:
        table = conn.table('USER_ACTION')
        for key, data in table.scan(row_start=(user_id + "_").encode(),
                                    row_stop=(user_id + "_" + str(9223372036854775807 - int(time.time()))),
                                    limit=10,
                                    columns=[b'PC:V']):
            user_views.append(int(data[b'PC:V'].decode()))
        conn.close()

    # 获得当前日期
    tim_str = datetime.datetime.now().strftime("%Y%m%d")
    global model_update_time, index

    try:
        if tim_str != model_update_time:
            model_update_time = tim_str
            # 重新读取模型文件
            index = faiss.read_index(faiss_model_path)
        vectors = np.array([index.reconstruct(spu) for spu in user_views])
        distance, item_index = index.search(vectors, num_items)
        # 合并去重
        item_list = [item for item in set(item_index.flatten("f").tolist()) if item not in user_views][:num_items]

    except Exception as e:
        item_list = []

    return item_list


def get_hot_item_recall(num_items):
    """
    基于时间窗口，推荐上一个小时段内的热门商品
    :param num_items:
    :return:
    """
    window_hour = (datetime.datetime.now() - datetime.timedelta(hours=0)).strftime('%Y%m%d%H')
    with pool.connection() as conn:
        table = conn.table('HOT_ITEM_STATIS')
        item_list = table.row(window_hour[::-1].encode(), columns=[b'INFO:HOT'])
        conn.close()
    item_list = eval(item_list[b'INFO:HOT'].decode())[:num_items] if item_list else []

    return item_list


def get_recall_integration(user_id, num_recall, num_user_profile_recall, num_itemcf_recall, num_recent_similar_recall):
    """
    召回融合，去重，过滤，增补
    :param user_id:
    :param num_user_profile_recall:
    :param num_itemcf_recall:
    :param num_recent_similar_recall:
    :return: item_list
    """
    user_profile_recall = get_user_profile_recall(user_id, num_user_profile_recall)
    itemcf_recall = get_itemcf_recall(user_id, num_itemcf_recall)
    recent_similar_recall = get_recent_similar_recall(user_id, num_recent_similar_recall)

    # 召回结果融合
    item_list = list(set(user_profile_recall + itemcf_recall + recent_similar_recall))

    # 读取浏览行为数据
    user_action_items = []
    with pool.connection() as conn:
        table = conn.table('USER_ACTION')
        for key, data in table.scan(row_start=(user_id + "_").encode(),
                                    row_stop=(user_id + "_" + str(9223372036854775807 - int(time.time()))),
                                    limit=5000,
                                    columns=[b'PC:V']):
            user_action_items.append(int(data[b'PC:V'].decode()))
        conn.close()

    # 过滤已看
    item_list = [item for item in item_list if item not in user_action_items]

    # 召回不足增补
    if len(item_list) < num_recall:
        item_list += get_hot_item_recall(num_recall - len(item_list))

    return item_list[:num_recall]


if __name__ == "__main__":
    res1 = get_user_profile_recall("test0"[::-1], 50)
    print("{0:-^60}\n".format("用户偏好召回：数量%d" % len(res1)), res1)

    res2 = get_itemcf_recall("test0"[::-1], 50)
    print("{0:-^60}\n".format("itemcf召回：数量%d" % len(res2)), res2)

    res3 = get_recent_similar_recall("test0"[::-1], 50)
    print("{0:-^60}\n".format("浏览相似召回：数量%d" % len(res3)), res3)

    res4 = get_hot_item_recall(50)
    print("{0:-^60}\n".format("热门召回：数量%d" % len(res4)), res4)

    res5 = get_recall_integration("test0"[::-1], 200, 60, 60, 60)
    print("{0:-^60}\n".format("召回融合：数量%d" % len(res5)), res5)

