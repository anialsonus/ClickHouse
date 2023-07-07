import pytest
import threading
import time
import sys
import os

from helpers.cluster import ClickHouseCluster

TEST_RUNS = 25
TEST_THREADS = 8
TEST_HALF_THREADS = 4
TEST_CLICKHOUSE_BENCHMARK_THREADS = 8
TEST_CLICKHOUSE_BENCHMARK_TRIES = 64

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/clusters.xml", "configs/session_log_config.xml"],
    user_configs=["configs/users.xml"],
    # with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/clusters.xml", "configs/session_log_config.xml"],
    user_configs=["configs/users.xml"],
    # with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class ThreadWithException(threading.Thread):
    run_exception = None

    def run(self):
        try:
            super().run()
        except:
            self.run_exception = sys.exc_info()

    def join(self):
        super().join()


def create_test_data():
    node1.query("CREATE USER IF NOT EXISTS 'test_user' IDENTIFIED BY 'pass'")
    node2.query("CREATE USER IF NOT EXISTS 'test_user' IDENTIFIED BY 'pass'")
    node1.query(
        "GRANT ALL ON *.* TO test_user",
    )
    node2.query("GRANT ALL ON *.* TO test_user")


def flush_records():
    node1.query("system flush logs")
    node2.query("system flush logs")


def run_queries(receive_timeout, times):
    run_queries_node(receive_timeout, node1, "node2", times)
    run_queries_node(receive_timeout, node2, "node1", times)


def run_queries_node(receive_timeout, node, remote_node, times):
    for i in range(times):
        node.query(f"set receive_timeout={receive_timeout}")
        node.query(
            f"select * from remote('{remote_node}', 'system', 'one', 'default', '')"
        )
        node.query_and_get_answer_with_error(
            f"select * from remote('{remote_node}', 'system', 'one', 'test_user', '')"
        )
        node.query(
            f"select * from remote('{remote_node}', 'system', 'one', 'test_user', 'pass')"
        )
        node.query_and_get_answer_with_error(
            f"select * from remote('{remote_node}', 'system', 'one', ' INTERSERVER SECRET ', 'pass')"
        )
        node.query(
            f"select * from remote('{remote_node}', 'system', 'one', 'default', '')"
        )
        node.query(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=default', LineAsString, 's String')"
        )
        node.query_and_get_answer_with_error(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=invalid', LineAsString, 's String')"
        )
        node.query_and_get_answer_with_error(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=test_user&password=wrong', LineAsString, 's String')"
        )
        node.query(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=test_user&password=pass', LineAsString, 's String')"
        )
        node.query("select * from cluster('cluster', 'system', 'one')")


def create_and_drop_phantom(node, sleep_time, times):
    for i in range(times):
        node.query("CREATE USER IF NOT EXISTS 'phantom'")
        time.sleep(sleep_time)
        node.query("DROP USER IF EXISTS 'phantom' ")
        time.sleep(sleep_time)


def run_phantom_queries_node(node, sleep_time, remote_node, times):
    for i in range(times):
        node.query(
            f"select * from remote('{remote_node}', 'system', 'one', 'phantom', '')",
            ignore_error=True,
        )
        node.query(
            f"select * from remote('{remote_node}', 'system', 'one', 'default', '')",
            user="phantom",
            ignore_error=True,
        )
        node.query(
            f"select * from remote('{remote_node}', 'system', 'one', 'phantom', '')",
            user="phantom",
            ignore_error=True,
        )

        node.query(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=phantom', LineAsString, 's String')",
            user="default",
            ignore_error=True,
        )
        node.query(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=phantom', LineAsString, 's String')",
            user="phantom",
            ignore_error=True,
        )
        node.query(
            f"select * from url('http://{remote_node}:8123/?query=select+1&user=default', LineAsString, 's String')",
            user="phantom",
            ignore_error=True,
        )

        time.sleep(sleep_time)


def run_benchmark(node, iterations, threads):
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "queries.sql"), "/queries.sql")

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"/usr/bin/clickhouse benchmark --ignore-error --iterations {iterations} --concurrency {threads} < /queries.sql",
        ],
        user="root",
        nothrow=True,
    )


def check_records():
    node1_invalid_records = node1.query(
        "select count(*) FROM system.session_log where client_port = 0 OR toString(client_address)='::ffff:0.0.0.0'"
    )
    node2_invalid_records = node2.query(
        "select count(*) FROM system.session_log where client_port = 0 OR toString(client_address)='::ffff:0.0.0.0'"
    )

    assert node1_invalid_records == "0\n"
    assert node2_invalid_records == "0\n"

    node1_valid_records = node1.query("select count(*) FROM system.session_log")
    node2_valid_records = node2.query("select count(*) FROM system.session_log")

    assert node1_valid_records != "0\n"
    assert node2_valid_records != "0\n"


def test_address_and_port_valid(started_cluster):
    create_test_data()

    for x in range(TEST_RUNS):
        for receive_timeout in [300, 1]:
            run_queries(receive_timeout, 1)

    flush_records()
    check_records()


def test_address_and_port_threaded(started_cluster):
    create_test_data()

    for x in range(TEST_RUNS):
        for receive_timeout in [300, 1]:
            thread_list = []
            for thx_id in range(TEST_THREADS):
                thread = ThreadWithException(
                    target=run_queries,
                    args=(
                        receive_timeout,
                        1,
                    ),
                )
                thread_list.append(thread)
                thread.start()

            for thread in thread_list:
                thread.join()

    flush_records()
    check_records()


def test_address_and_port_threaded_times(started_cluster):
    create_test_data()

    for receive_timeout in [300, 1]:
        thread_list = []
        for thx_id in range(TEST_THREADS):
            thread = ThreadWithException(
                target=run_queries,
                args=(
                    receive_timeout,
                    TEST_RUNS,
                ),
            )
            thread_list.append(thread)
            thread.start()

        for thread in thread_list:
            thread.join()

    flush_records()
    check_records()


def test_address_and_port_threaded_separate(started_cluster):
    create_test_data()

    for x in range(TEST_RUNS):
        for receive_timeout in [300, 1]:
            thread_list = []
            for thx_id in range(TEST_HALF_THREADS):
                thread = ThreadWithException(
                    target=run_queries_node,
                    args=(
                        receive_timeout,
                        node1,
                        "node2",
                        1,
                    ),
                )
                thread_list.append(thread)
                thread.start()
                thread = ThreadWithException(
                    target=run_queries_node,
                    args=(
                        receive_timeout,
                        node2,
                        "node1",
                        1,
                    ),
                )
                thread_list.append(thread)
                thread.start()
                thread = ThreadWithException(
                    target=run_queries_node,
                    args=(
                        receive_timeout,
                        node1,
                        "node1",
                        1,
                    ),
                )
                thread_list.append(thread)
                thread.start()

            for thread in thread_list:
                thread.join()

    flush_records()
    check_records()


def test_address_and_port_threaded_separate_times(started_cluster):
    create_test_data()
    for receive_timeout in [300, 1]:
        thread_list = []
        for thx_id in range(TEST_HALF_THREADS):
            thread = ThreadWithException(
                target=run_queries_node,
                args=(
                    receive_timeout,
                    node1,
                    "node2",
                    TEST_RUNS,
                ),
            )
            thread_list.append(thread)
            thread.start()
            thread = ThreadWithException(
                target=run_queries_node,
                args=(
                    receive_timeout,
                    node2,
                    "node1",
                    TEST_RUNS,
                ),
            )
            thread_list.append(thread)
            thread.start()

        for thread in thread_list:
            thread.join()

    flush_records()
    check_records()


def test_address_and_port_threaded_concurrent_delete_and_create_user(started_cluster):
    create_test_data()
    for receive_timeout in [300, 1]:
        thread_list = []
        thread = ThreadWithException(
            target=create_and_drop_phantom,
            args=(
                node1,
                0.66,
                TEST_RUNS,
            ),
        )
        thread_list.append(thread)
        thread.start()
        time.sleep(0.7)
        thread = ThreadWithException(
            target=create_and_drop_phantom,
            args=(
                node2,
                1.1,
                TEST_RUNS,
            ),
        )
        thread_list.append(thread)
        thread.start()

        for thx_id in range(TEST_HALF_THREADS):
            thread = ThreadWithException(
                target=run_phantom_queries_node,
                args=(
                    node1,
                    0.8,
                    "node2",
                    TEST_RUNS,
                ),
            )
            thread_list.append(thread)
            thread.start()
            thread = ThreadWithException(
                target=run_phantom_queries_node,
                args=(
                    node2,
                    0.33,
                    "node1",
                    TEST_RUNS,
                ),
            )
            thread_list.append(thread)
            thread.start()
            thread = ThreadWithException(
                target=run_phantom_queries_node,
                args=(
                    node1,
                    0.27,
                    "node1",
                    TEST_RUNS,
                ),
            )
            thread_list.append(thread)
            thread.start()
            time.sleep(0.139)

        for thread in thread_list:
            thread.join()

    flush_records()
    check_records()


def test_clickhouse_benchmark(started_cluster):
    thread_list = []
    thread = ThreadWithException(
        target=run_benchmark,
        args=(
            node1,
            TEST_CLICKHOUSE_BENCHMARK_TRIES,
            TEST_CLICKHOUSE_BENCHMARK_THREADS,
        ),
    )

    thread_list.append(thread)
    thread.start()

    thread = ThreadWithException(
        target=run_benchmark,
        args=(
            node2,
            TEST_CLICKHOUSE_BENCHMARK_TRIES,
            TEST_CLICKHOUSE_BENCHMARK_THREADS,
        ),
    )
    thread_list.append(thread)
    thread.start()

    for thread in thread_list:
        thread.join()

    flush_records()
    check_records()
