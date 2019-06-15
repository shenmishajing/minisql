import record_manager


class API:
    def __init__(self, block_size, memory_size, work_dir='.'):
        self.record_manager = record_manager.record_manager(block_size, memory_size, work_dir)

    def exec_sql(self, sql):
        pass
