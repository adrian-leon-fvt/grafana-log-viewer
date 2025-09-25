LIVE_STREAMING = False

TEST_MF4_FILE = "/home/default/work/test_dbc/00000007.MF4"
DBC_FILE_PATHS = [
    "/home/default/work/test_dbc/d65_brightloops.dbc"
]

CAN_INTERFACE = 'socketcan'
CAN_CHANNEL = 'vcan0'

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = "[%(asctime)s] %(filename)s:%(lineno)d %(message)s"


vm_import_url = "http://victoriametrics.tail696b12.ts.net:8428/api/v1/import/prometheus"
vm_export_url = "http://victoriametrics.tail696b12.ts.net:8428/api/v1/export"
vm_query_url = "http://victoriametrics.tail696b12.ts.net:8428/api/v1/query"
vm_query_range_url = "http://victoriametrics.tail696b12.ts.net:8428/api/v1/query_range"

# vm_import_url = "http://localhost:8428/api/v1/import/prometheus"
# vm_export_url = "http://localhost:8428/api/v1/export"
# vm_query_url = "http://localhost:8428/api/v1/query"
# vm_query_range_url = "http://localhost:8428/api/v1/query_range"